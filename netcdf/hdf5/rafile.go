package hdf5

import (
	"io"
)

// Random access file: can do random seeks
type raFile struct {
	rcFile      *refCountedFile // ref counted file
	seekPointer int64
}

func newRaFile(file io.ReadSeeker) *raFile {
	var f raFile
	f.rcFile = newRefCountedFile(file)
	f.seekPointer = 0
	return &f
}

func (f *raFile) Close() error {
	return f.rcFile.dereference()
}

func (f *raFile) seekAt(offset int64) *raFile {
	r := *f
	r.seekPointer = offset
	return &r
}

func (f *raFile) dup() *raFile {
	r := *f
	r.rcFile.reference()
	return &r
}

func (f *raFile) Read(p []byte) (int, error) {
	// Do read
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
	_, err := f.rcFile.file.Seek(offset, io.SeekStart)
	if err != nil {
		logger.Error("Seek error in ReadAt", err, offset, io.SeekStart)
		return 0, err
	}
	n, retErr := f.rcFile.file.Read(b)
	if retErr != nil {
		logger.Error("Read error in ReadAt", err, offset, io.SeekStart)
	}
	_, err = f.rcFile.file.Seek(f.seekPointer, io.SeekStart)
	if err != nil {
		logger.Error("Seek error in ReadAt (reset)", err, offset, io.SeekStart)
		if retErr == nil {
			retErr = err
		}
	}
	return n, retErr
}

type refCountedFile struct {
	file     io.ReadSeeker
	refCount int
}

func newRefCountedFile(file io.ReadSeeker) *refCountedFile {
	return &refCountedFile{file, 1}
}

func (rcf *refCountedFile) reference() {
	rcf.refCount++
}

func (rcf *refCountedFile) dereference() error {
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
