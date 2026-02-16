package hdf5

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/batchatco/go-thrower"
)

type floatingPointManagerType struct{}

var (
	floatingPointManager             = floatingPointManagerType{}
	_                    typeManager = floatingPointManager
)

func (floatingPointManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	switch attr.length {
	case 4:
		return "float"
	case 8:
		return "double"
	default:
		panic("bad fp length")
	}
}

func (floatingPointManagerType) goTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	switch attr.length {
	case 4:
		return "float32"
	case 8:
		return "float64"
	default:
		panic("bad fp length")
	}
}

func (floatingPointManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) any {
	var values any
	switch attr.length {
	case 4:
		values = allocFloats(bf, dimensions, attr.endian)
		logger.Info("done alloc floats, rem=", bf.(remReader).Rem())
	case 8:
		values = allocDoubles(bf, dimensions, attr.endian)
	default:
		fail(fmt.Sprintf("bad size float: %d", attr.length))
	}
	return values // already converted
}

func (floatingPointManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	switch obj.objAttr.length {
	case 4:
		var fv float32
		if undefinedFillValue {
			fv = float32(math.NaN())
		}
		var buf bytes.Buffer
		err := binary.Write(&buf, obj.objAttr.endian, &fv)
		thrower.ThrowIfError(err)
		objFillValue = buf.Bytes()
		logger.Info("fill value encoded", objFillValue)
	case 8:
		var fv float64
		if undefinedFillValue {
			fv = math.NaN()
		}
		var buf bytes.Buffer
		err := binary.Write(&buf, obj.objAttr.endian, &fv)
		thrower.ThrowIfError(err)
		objFillValue = buf.Bytes()
		logger.Info("fill value encoded", objFillValue)
	default:
		thrower.Throw(ErrInternal)
	}
	return objFillValue
}

func (floatingPointManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	assertError(attr.dtversion == 1, ErrFloatingPoint, "Only support version 1 of float")
	logger.Info("* floating-point")
	endian := ((bitFields >> 5) & 0b10) | (bitFields & 0b1)
	switch endian {
	case 0:
		attr.endian = binary.LittleEndian
	case 1:
		attr.endian = binary.BigEndian
	default:
		fail(fmt.Sprint("unhandled byte order: ", endian))
	}
	loPad := (bitFields & 0b10) == 0b10
	assertError(!loPad, ErrFloatingPoint, "low pad not supported")
	hiPad := (bitFields & 0b100) == 0b100
	assertError(!hiPad, ErrFloatingPoint, "high pad not supported")
	intPad := (bitFields & 0b1000) == 0b1000
	assertError(!intPad, ErrFloatingPoint, "internal pad not supported")
	mantissaNormalization := (bitFields >> 4) & 0b11
	logger.Info("* mantissa normalization:", mantissaNormalization)
	sign := (bitFields >> 8) & 0b11111111
	logger.Info("* sign: ", sign)
	assert(bf.Rem() >= 12,
		fmt.Sprint("Properties need to be at least 12 bytes, was ", bf.Rem()))
	bitOffset := read16(bf)
	bitPrecision := read16(bf)
	exponentLocation := read8(bf)
	exponentSize := read8(bf)
	mantissaLocation := read8(bf)
	mantissaSize := read8(bf)
	exponentBias := read32(bf)

	logger.Infof("* bitOffset=%d bitPrecision=%d exponentLocation=%d exponentSize=%d mantissaLocation=%d mantissaSize=%d exponentBias=%d",
		bitOffset,
		bitPrecision,
		exponentLocation,
		exponentSize,
		mantissaLocation,
		mantissaSize,
		exponentBias)
	assertError(bitOffset == 0, ErrFloatingPoint, "bit offset must be zero")
	assertError(mantissaNormalization == 2, ErrFloatingPoint, "mantissa normalization must be 2")
	switch attr.length {
	case 4:
		assertError(sign == 31, ErrFloatingPoint, "float32 sign location must be 31")
		assertError(bitPrecision == 32, ErrFloatingPoint, "float32 precision must be 32")
		assertError(exponentLocation == 23, ErrFloatingPoint, "float32 exponent location must be 23")
		assertError(exponentSize == 8, ErrFloatingPoint, "float32 exponent size must be 8")
		assertError(exponentBias == 127, ErrFloatingPoint, "float32 exponent bias must be 127")
	case 8:
		assertError(sign == 63, ErrFloatingPoint, "float64 sign location must be 63")
		assertError(bitPrecision == 64, ErrFloatingPoint, "float64 precision must be 64")
		assertError(exponentLocation == 52, ErrFloatingPoint, "float64 exponent location must be 52")
		assertError(exponentSize == 11, ErrFloatingPoint, "float64 exponent size must be 11")
		assertError(exponentBias == 1023, ErrFloatingPoint, "float64 exponent bias must be 1023")
	default:
		logger.Error("bad dtlenth for fp", attr.length)
		thrower.Throw(ErrFloatingPoint)
	}
	if df == nil {
		logger.Infof("no data")
		return
	}
	logger.Info("data len", df.Rem())
	assert(df.Rem() >= int64(attr.length), "floating-point data short")
	attr.df = newResetReaderSave(df, df.Rem())
}

func allocFloats(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder) any {
	if len(dimLengths) == 0 {
		var value float32
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]float32, thisDim)
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(reflect.TypeOf(float32(0)), dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocFloats(bf, dimLengths[1:], endian)))
	}
	return vals.Interface()
}

func allocDoubles(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder) any {
	if len(dimLengths) == 0 {
		var value float64
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]float64, thisDim)
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(reflect.TypeOf(float64(0)), dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocDoubles(bf, dimLengths[1:], endian)))
	}
	return vals.Interface()
}
