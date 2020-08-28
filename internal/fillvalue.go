// Internal API, not to be exported
package internal

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
	rl := len(fvr.repeat)
	ri := fvr.repeatIndex
	z := p
	if ri == 0 {
		for i := 0; i < len(p)-rl+1; i += rl {
			copy(z, fvr.repeat)
			z = z[rl:]
		}
	}
	for i := 0; i < len(z); i++ {
		z[i] = fvr.repeat[ri%rl]
		ri++
	}
	fvr.repeatIndex = ri % rl
	return len(p), nil
}
