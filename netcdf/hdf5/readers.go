package hdf5

// Various readers here

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/batchatco/go-thrower"
)

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

// newFletcher32reader creates a reader implementing the fletcher32 algorithm.
func newFletcher32Reader(r io.Reader, size uint64) remReader {
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
		case fl.count%2 == 1:
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
		if fl.size%2 == 1 {
			// Retrieve partial and complete sum as if next byte were zero.
			val := uint16(fl.partial) << 8
			addToSums(val)
		}
		calcedSum := (uint32(fl.sum2) << 16) | uint32(fl.sum1)
		if !fl.readChecksum {
			binary.Read(fl.r, binary.LittleEndian, &fl.checksum)
		}
		if calcedSum != fl.checksum {
			logger.Infof("checksum failure: sum=%#x file sum=%#x\n", calcedSum,
				fl.checksum)
			thrower.Throw(ErrFletcherChecksum)
		}
	}
	return n, nil
}

// oldFletcher32reader creates a reader implementing the fletcher32 algorithm.
// It is inefficient and is here for testing purposes only.
func oldFletcher32Reader(r io.Reader, size uint64) remReader {
	assert(size >= 4, "bad size for fletcher")
	b := make([]byte, size-4)
	read(r, b)
	var checksum uint32
	binary.Read(r, binary.LittleEndian, &checksum)
	bf := newResetReaderFromBytes(b)
	values := make([]uint16, len(b)/2)
	binary.Read(bf, binary.BigEndian, values)
	if len(b)%2 == 1 {
		last := uint16(b[len(b)-1])
		values = append(values, last<<8)
	}
	calcedSum := fletcher32(values)
	if calcedSum != checksum {
		logger.Infof("checksum failure: calced sum=%#x filesum=%#x\n", calcedSum, checksum)
		thrower.Throw(ErrFletcherChecksum)
	}
	return newResetReaderFromBytes(b)
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
	for i := 0; i < int(n); i++ {
		for j := 0; j < nelems; j++ {
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
