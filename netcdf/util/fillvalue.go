package util

import (
	"io"
)

type FillValueReader struct {
	repeat      []byte
	repeatIndex int
}

func NewFillValueReader(repeat []byte) io.Reader {
	return &FillValueReader{repeat, 0}
}

func (fvr *FillValueReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = fvr.repeat[fvr.repeatIndex]
		fvr.repeatIndex = (fvr.repeatIndex + 1) % len(fvr.repeat)
	}
	return len(p), nil
}
