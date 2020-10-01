package hdf5

import (
	"encoding/binary"
	"io"

	"github.com/batchatco/go-thrower"
)

type bitfieldManagerType struct{}

var (
	bitfieldManager             = bitfieldManagerType{}
	_               typeManager = bitfieldManager
)

func (bitfieldManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	return "uchar" // same as uint8
}

func (bitfieldManagerType) goTypeString(sh sigHelper, typeName string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	return "uint8" // bitfield same as uint8
}

func (bitfieldManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	values := allocInt8s(bf, dimensions, false, nil)
	return values
}

func (bitfieldManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (bitfieldManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	endian := hasFlag8(uint8(bitFields), 0)
	switch endian {
	case false:
		attr.endian = binary.LittleEndian
	case true:
		attr.endian = binary.BigEndian
	}
	loPad := hasFlag8(uint8(bitFields), 1)
	assert(!loPad, "low pad not supported")
	hiPad := hasFlag8(uint8(bitFields), 2)
	assert(!hiPad, "high pad not supported")
	bitOffset := read16(bf)
	checkVal(0, bitOffset, "bit offset must be zero")
	bitPrecision := read16(bf)
	logger.Infof("BitField offset %d, precision %d", bitOffset, bitPrecision)
	if df == nil || df.Rem() == 0 {
		logger.Infof("no data")
		return
	}
	logger.Info("bitfield rem: ", df.Rem())
	if !allowBitfields {
		if df != nil {
			b := make([]byte, df.Rem())
			read(df, b)
			logger.Infof("bitfield value: %#x", b)
		}
		logger.Infof("Bitfields ignored")
		thrower.Throw(ErrBitfield)
	}
	if df.Rem() >= int64(attr.length) {
		attr.df = newResetReaderSave(df, df.Rem())
	}
}
