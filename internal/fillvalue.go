// Internal API, not to be exported
package internal

import (
	"io"
)

const (
	// NetCDF default fill values as defined in the NetCDF4 specification
	FillByte   int8   = -127
	FillChar   uint8  = 0
	FillShort  int16  = -32767
	FillInt    int32  = -2147483647
	FillFloat  uint32 = 0x7cf00000
	FillDouble uint64 = 0x479e000000000000

	// NetCDF additions defined in the CDF-5 specification
	FillUByte  uint8  = 255
	FillUShort uint16 = 65535
	FillUInt   uint32 = 4294967295
	FillInt64  int64  = -9223372036854775806
	FillUInt64 uint64 = 0xfffffffffffffffe
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
	for i := range z {
		z[i] = fvr.repeat[ri%rl]
		ri++
	}
	fvr.repeatIndex = ri % rl
	return len(p), nil
}
