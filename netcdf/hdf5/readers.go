package hdf5

// Various readers here

import (
	"bufio"
	"bytes"
	"io"
	"sync"

	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
)

func skip(r io.Reader, length int64) {
	_, err := io.CopyN(io.Discard, r, length)
	thrower.ThrowIfError(err)
}

// remReader remembers the count (used) and remaining bytes
type remReader interface {
	io.Reader
	Count() int64
	Rem() int64
}

// refCountedFile allows multiple readers on the same file handle
type refCountedFile struct {
	file     io.ReadSeeker
	refCount int
	lock     sync.Mutex // TODO: do we need this lock?
}

// Random access file: can do random seeks
type raFile struct {
	rcFile      *refCountedFile // ref counted file
	seekPointer int64
}

// resetreader takes a file and a size (or a byte array), and
// returns a remReader for it.
type resetReader struct {
	remReader
	lr    *io.LimitedReader
	size  int64
	bytes []byte
}

// unshuffleReader implements the shuffle algorithm
type unshuffleReader struct {
	r            io.Reader
	b            []byte
	size         uint64
	shuffleParam uint32
}

// Streaming version of a fletcher32 reader
type fletcher struct {
	r            io.Reader
	size         uint64
	count        uint64
	sum1         uint32
	sum2         uint32
	partial      uint8
	checksum     uint32
	readChecksum bool
}

// fletcher32Reader creates a reader implementing the fletcher32 algorithm.
func fletcher32Reader(r io.Reader, size uint64) remReader {
	assert(size >= 4, "bad size for fletcher")
	return &fletcher{
		r:            r,
		size:         size,
		count:        0,
		sum1:         0,
		sum2:         0,
		partial:      0,
		checksum:     0,
		readChecksum: false,
	}
}

func (fl *fletcher) Rem() int64 {
	return int64((fl.size - 4) - fl.count)
}

func (fl *fletcher) Size() int64 {
	return int64(fl.size)
}

func (fl *fletcher) Count() int64 {
	return int64(fl.count)
}

func (fl *fletcher) Read(b []byte) (int, error) {
	thisLen := uint64(len(b))
	if thisLen+fl.count > fl.size-4 {
		thisLen = (fl.size - 4) - fl.count
	}
	n, err := fl.r.Read(b[:thisLen])
	if err != nil && err != io.EOF {
		return 0, err
	}
	addToSums := func(val uint16) {
		fl.sum1 = (fl.sum1 + uint32(val)) % 65535
		fl.sum2 = (fl.sum1 + fl.sum2) % 65535
	}
	for i := 0; i < n; i++ {
		var val uint16
		switch {
		case (fl.count+uint64(i))%2 == 1:
			// Previous read left us with an odd count.
			// Recover the partial read and compute val.
			val = (uint16(fl.partial) << 8) | uint16(b[i])
			addToSums(val)
		case i == n-1:
			// Can't complete a read, so save the partial.
			fl.partial = b[i]
		default:
			val = (uint16(b[i]) << 8) | uint16(b[i+1])
			addToSums(val)
			i++
		}
	}
	fl.count += uint64(n)
	if fl.count == fl.size-4 {
		if (fl.size-4)%2 == 1 {
			// Retrieve partial and complete sum as if next byte were zero.
			val := uint16(fl.partial) << 8
			addToSums(val)
		}
		calcedSum := (uint32(fl.sum2) << 16) | uint32(fl.sum1)
		if !fl.readChecksum {
			util.MustReadLE(fl.r, &fl.checksum)
			fl.readChecksum = true
		}
		if calcedSum != fl.checksum {
			thrower.Throw(ErrFletcherChecksum)
		}
	}
	return n, nil
}

func (r *resetReader) Rem() int64 {
	return r.lr.N
}

func (r *resetReader) Count() int64 {
	return r.size - r.lr.N
}

func (r *resetReader) Read(p []byte) (int, error) {
	return r.lr.Read(p)
}

func newResetReaderFromBytes(b []byte) remReader {
	file := bytes.NewReader(b)
	size := int64(len(b))
	ret := &resetReader{
		lr:    &io.LimitedReader{R: file, N: size},
		size:  size,
		bytes: b,
	}
	return ret
}

func newResetReader(file io.Reader, size int64) remReader {
	ret := &resetReader{
		lr:   &io.LimitedReader{R: file, N: size},
		size: size,
	}
	return ret
}

func newResetReaderSave(file io.Reader, size int64) remReader {
	b := make([]byte, size)
	read(file, b)
	return newResetReaderFromBytes(b)
}

func newResetReaderOffset(file *raFile, size int64, offset uint64) remReader {
	ra := file.seekAt(int64(offset))
	bf := bufio.NewReader(ra)
	ret := &resetReader{
		lr:   &io.LimitedReader{R: bf, N: size},
		size: size,
	}
	return ret
}

func newRaFile(file io.ReadSeeker) *raFile {
	return &raFile{
		rcFile:      newRefCountedFile(file),
		seekPointer: 0,
	}
}

func (f *raFile) Close() error {
	return f.rcFile.dereference()
}

func (f *raFile) seekAt(offset int64) *raFile {
	return &raFile{
		rcFile:      f.rcFile,
		seekPointer: offset,
	}
}

func (f *raFile) dup() *raFile {
	f.rcFile.reference()
	return &raFile{
		rcFile:      f.rcFile,
		seekPointer: f.seekPointer,
	}
}

func (f *raFile) Read(p []byte) (int, error) {
	// Do read
	f.rcFile.Lock()
	defer f.rcFile.Unlock()
	return f.readNoLock(p)
}

func (f *raFile) readNoLock(p []byte) (int, error) {
	thisLen := len(p)
	_, err := f.rcFile.file.Seek(f.seekPointer, io.SeekStart)
	if err != nil {
		logger.Error("Seek error in Read", err, io.SeekStart)
		return 0, err
	}
	n, err := f.rcFile.file.Read(p[:thisLen])
	if err != nil {
		logger.Error("Read error in Read", err, io.SeekStart)
		return 0, err
	}
	// Save position
	f.seekPointer += int64(n)
	// Return read results
	return n, err
}

func (f *raFile) ReadAt(b []byte, offset int64) (int, error) {
	f.rcFile.Lock()
	defer f.rcFile.Unlock()
	save := f.seekPointer
	_, err := f.rcFile.file.Seek(offset, io.SeekStart)
	if err != nil {
		logger.Error("Seek error in ReadAt", err, offset, io.SeekStart)
		return 0, err
	}
	f.seekPointer = offset
	n, retErr := f.readNoLock(b)
	f.seekPointer = save
	return n, retErr
}

func newRefCountedFile(file io.ReadSeeker) *refCountedFile {
	return &refCountedFile{file, 1, sync.Mutex{}}
}

func (rcf *refCountedFile) reference() {
	rcf.refCount++
}

func (rcf *refCountedFile) Lock() {
	rcf.lock.Lock()
}

func (rcf *refCountedFile) Unlock() {
	rcf.lock.Unlock()
}

func (rcf *refCountedFile) dereference() error {
	rcf.lock.Lock()
	defer rcf.lock.Unlock()
	var err error
	rcf.refCount--
	switch {
	case rcf.refCount == 0:
		logger.Info("Closing file")
		if f, ok := rcf.file.(io.Closer); ok {
			f.Close()
		}
		rcf.file = nil
	case rcf.refCount < 0:
		err = ErrInternal
	}
	return err
}

func newUnshuffleReader(r io.Reader, size uint64, shuffleParam uint32) remReader {
	return newResetReader(&unshuffleReader{r, nil, size, shuffleParam}, int64(size))
}

func unshuffle(val []byte, n uint32) {
	if n == 1 {
		return // avoids allocation
	}
	// shuffle params are never very large in practice, so this isn't wasteful
	// of memory.
	tmp := make([]byte, len(val))
	nelems := len(val) / int(n)
	for i := range int(n) {
		for j := range nelems {
			tmp[j*int(n)+i] = val[i*nelems+j]
		}
	}
	copy(val, tmp)
}

func (r *unshuffleReader) Read(p []byte) (int, error) {
	if r.size == 0 {
		return 0, io.EOF
	}
	thisLen := uint64(len(p))
	if thisLen > r.size {
		thisLen = r.size
	}
	var err error
	if r.b == nil {
		r.b = make([]byte, r.size)
		tot, err := readAll(r.r, r.b)
		unshuffle(r.b[:tot], r.shuffleParam)
		if err != nil {
			assert(err == io.EOF, err.Error())
		}
	}
	copy(p, r.b[:thisLen])
	r.b = r.b[thisLen:]
	r.size -= thisLen
	if r.size == 0 {
		return int(thisLen), io.EOF
	}
	return int(thisLen), err
}

type layoutReader struct {
	r           remReader
	obj         *object
	buf         []byte
	size        int64
	count       uint64 // number bytes produced
	stripeIndex int
}

// calcChunkSize calculates the size of a chunk
// Returns 0 if not chunked
func calcChunkSize(attr *attribute) uint64 {
	chunkSize := uint64(1)
	for _, v := range attr.layout {
		chunkSize *= v
	}
	return chunkSize
}

// calcAttrSize computes the total size of an object in bytes.
// If it is chunked, it may be larger than the data size.
func calcAttrSize(attr *attribute) uint64 {
	dtLength := uint64(attr.length)
	if len(attr.layout) == 0 {
		return calcDataSize(attr) * dtLength // not chunked
	}
	totalChunks := uint64(1)
	for i := range attr.dimensions {
		gi := (attr.dimensions[i] + attr.layout[i] - 1) / attr.layout[i]
		totalChunks *= gi
	}
	chunkSize := calcChunkSize(attr)
	return totalChunks * chunkSize * dtLength
}

// calcDataSize computes the size of the data, not counting chunks.
func calcDataSize(attr *attribute) uint64 {
	dataSize := uint64(1)
	for _, v := range attr.dimensions {
		dataSize *= v
	}
	return dataSize
}

func calculateFlatOffset(offsets []uint64, dims []uint64, layout []uint64, dtLength uint64) uint64 {
	rank := len(dims)
	flatChunkIndex := uint64(0)
	for i := range rank {
		gi := offsets[i] / layout[i]
		gridStride := uint64(1)
		for k := i + 1; k < rank; k++ {
			gridStride *= (dims[k] + layout[k] - 1) / layout[k]
		}
		flatChunkIndex += gi * gridStride
	}

	chunkSize := uint64(1)
	for _, v := range layout {
		chunkSize *= v
	}

	return flatChunkIndex * chunkSize * dtLength
}

func (lr *layoutReader) fillrow() error {
	attr := lr.obj.objAttr
	dims := attr.dimensions
	layout := attr.layout
	dtLength := uint64(attr.length)
	rank := len(dims)

	// Grid dimensions
	g := make([]int, rank)
	totalStripes := 1
	for i := range rank {
		g[i] = int((dims[i] + layout[i] - 1) / layout[i])
		if i < rank-1 {
			totalStripes *= g[i]
		}
	}

	if lr.stripeIndex >= totalStripes {
		return io.EOF
	}

	// Current stripe grid coordinates
	stripeCoords := make([]int, rank-1)
	tempIndex := lr.stripeIndex
	for i := rank - 2; i >= 0; i-- {
		stripeCoords[i] = tempIndex % g[i]
		tempIndex /= g[i]
	}

	// Calculate number of valid rows in this stripe
	// A "row" here is the last dimension.
	numRowsInStripe := 1
	validCounts := make([]int, rank-1)
	for i := range rank-1 {
		start := uint64(stripeCoords[i]) * layout[i]
		end := start + layout[i]
		if end > dims[i] {
			end = dims[i]
		}
		validCounts[i] = int(end - start)
		numRowsInStripe *= validCounts[i]
	}

	// Stripe buffer size
	lineLen := dims[rank-1]
	lr.buf = make([]byte, uint64(numRowsInStripe)*lineLen*dtLength)

	// Read one stripe of chunks
	numChunksInStripe := g[rank-1]
	chunkSize := calcChunkSize(attr)
	readBuf := make([]byte, chunkSize*dtLength)

	for gc := range numChunksInStripe {
		// Read one chunk
		_, err := io.ReadFull(lr.r, readBuf)
		if err != nil {
			return err
		}

		// Interleave this chunk's rows into lr.buf
		numRowsInChunk := 1
		for i := range rank-1 {
			numRowsInChunk *= int(layout[i])
		}

		chunkLineLen := layout[rank-1]
		delta := make([]int, rank-1)

		for rc := range numRowsInChunk {
			// Decode rc into local relative coordinates
			tempRC := rc
			for i := rank - 2; i >= 0; i-- {
				delta[i] = tempRC % int(layout[i])
				tempRC /= int(layout[i])
			}

			// Check if this row is valid in the dataset
			isValid := true
			for i := range rank-1 {
				if uint64(stripeCoords[i])*layout[i]+uint64(delta[i]) >= dims[i] {
					isValid = false
					break
				}
			}

			if !isValid {
				continue
			}

			destRowIndex := 0
			stride := 1
			for i := rank - 2; i >= 0; i-- {
				destRowIndex += delta[i] * stride
				stride *= validCounts[i]
			}

			srcOffset := uint64(rc) * chunkLineLen * dtLength
			destColOffset := uint64(gc) * layout[rank-1]
			if destColOffset >= lineLen {
				continue
			}

			destOffset := (uint64(destRowIndex)*lineLen + destColOffset) * dtLength
			copyLen := chunkLineLen
			if destColOffset+copyLen > lineLen {
				copyLen = lineLen - destColOffset
			}

			copy(lr.buf[destOffset:destOffset+copyLen*dtLength], readBuf[srcOffset:srcOffset+copyLen*dtLength])
		}
	}

	lr.stripeIndex++
	return nil
}

func (lr *layoutReader) Read(b []byte) (int, error) {
	if len(lr.buf) == 0 {
		err := lr.fillrow()
		if err != nil {
			return 0, err
		}
	}

	n := copy(b, lr.buf)
	lr.buf = lr.buf[n:]
	lr.count += uint64(n)
	return n, nil
}

func (lr *layoutReader) Rem() int64 {
	return lr.size - int64(lr.count)
}

func (lr *layoutReader) Count() int64 {
	return int64(lr.count)
}

func newLayoutReader(r remReader, obj *object) io.Reader {
	size := calcDataSize(obj.objAttr) * uint64(obj.objAttr.length)
	return &layoutReader{r, obj, nil, int64(size), 0, 0}
}
