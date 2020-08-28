package hdf5

import (
	"bytes"
	"io"
	"sync"
)

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

type resetReader struct {
	remReader
	lr   *io.LimitedReader
	size int64
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
	return newResetReader(bytes.NewReader(b), int64(len(b)))
}

func newResetReader(file io.Reader, size int64) remReader {
	return &resetReader{
		lr:   &io.LimitedReader{R: file, N: size},
		size: size}
}

type holeReader struct {
	r         io.Reader
	size      int64
	count     int64
	totalSize int64
}

func (r *holeReader) Rem() int64 {
	return r.totalSize - r.count
}

func (r *holeReader) Count() int64 {
	return r.count
}

func (r *holeReader) Read(p []byte) (int, error) {
	thisLen := int64(len(p))
	if r.count+thisLen > r.size {
		if r.count < r.size {
			thisLen = r.size - r.count
		} else {
			thisLen = 0
		}
	}
	var nRead int64
	if thisLen > 0 {
		// regular read
		n, err := r.r.Read(p[:thisLen])
		if err != nil {
			return 0, err
		}
		r.count += int64(n)
		if int64(n) == thisLen {
			return n, err
		}
		r.count += int64(n)
		nRead = int64(n)
		// more to read
	}
	// a hole
	thisLen = int64(len(p)) - nRead
	if thisLen+r.count > r.totalSize {
		thisLen = r.totalSize - r.count
		if thisLen > int64(len(p)) {
			thisLen = int64(len(p))
		}
		r.count += thisLen
		return int(thisLen), nil
	}
	if thisLen == 0 {
		return 0, io.EOF
	}
	r.count += thisLen
	return int(thisLen), nil
}

func newHoleReader(file io.Reader, size int64, totalSize int64) *holeReader {
	return &holeReader{file, size, 0, totalSize}
}

func newSkipReader(file io.Reader, size int64, skip int64, totalSize int64) (remReader, error) {
	bf := newResetReader(file, size+skip)
	// Fake a seek
	for skip > 0 {
		skipSize := int64(1024 * 1024)
		if skipSize > skip {
			skipSize = skip
		}
		b := make([]byte, skipSize) // read a megabyte at a time to skip
		n, err := bf.Read(b)
		if err != nil {
			return nil, err
		}
		if n < len(b) {
			return nil, io.EOF
		}
		skip -= int64(n)
	}
	return newHoleReader(bf, size, totalSize), nil
}

func newRaFile(file io.ReadSeeker) *raFile {
	return &raFile{
		rcFile:      newRefCountedFile(file),
		seekPointer: 0}
}

func (f *raFile) Close() error {
	return f.rcFile.dereference()
}

func (f *raFile) seekAt(offset int64) *raFile {
	return &raFile{
		rcFile:      f.rcFile,
		seekPointer: offset}
}

func (f *raFile) dup() *raFile {
	f.rcFile.reference()
	return &raFile{
		rcFile:      f.rcFile,
		seekPointer: f.seekPointer}
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
