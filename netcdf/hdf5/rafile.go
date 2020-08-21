package hdf5

import (
	"bytes"
	"io"
	"math"
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
	io.Reader
	lr   *io.LimitedReader
	size int64
	zr   io.Reader // zero reader, when don't have lr
	n    int64     // nread for zero reader
}

func (r *resetReader) Rem() int64 {
	if r.zr != nil {
		return r.n
	}
	return r.lr.N
}

func (r *resetReader) Count() int64 {
	if r.zr != nil {
		return r.size - r.n
	}
	return r.size - r.lr.N
}

func (r *resetReader) Read(p []byte) (int, error) {
	if r.zr != nil {
		n, err := r.zr.Read(p)
		if err != nil {
			//panic("unexpected error 1")
			return n, err
		}
		r.n += int64(n)
		return n, nil
	}
	n, err := r.lr.Read(p)
	if err != nil {
		//panic("unexpected error 2")
		return n, err
	}
	return n, nil
}

func newResetReaderFromBytes(b []byte) *resetReader {
	return newResetReader(bytes.NewReader(b), int64(len(b)))
}

func newResetReader(file io.Reader, size int64) *resetReader {
	if size == 0 {
		return &resetReader{
			lr:   nil,
			zr:   file,
			size: math.MaxInt32}
	}
	var f io.Reader
	if size < 0 {
		if size < 0 {
			size = -size
		}
		b := make([]byte, size)
		read(file, b)
		f = bytes.NewReader(b)
	} else {
		f = file
	}
	return &resetReader{
		lr:   &io.LimitedReader{R: f, N: size},
		size: size,
		zr:   nil,
		n:    0}
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
