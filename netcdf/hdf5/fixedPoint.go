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

type fixedPointManagerType struct{}

var (
	fixedPointManager             = fixedPointManagerType{}
	_                 typeManager = fixedPointManager
)

func (fixedPointManagerType) cdlTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	prefix := ""
	if !attr.signed {
		prefix = "u"
	}
	switch attr.length {
	case 1:
		return prefix + "byte"
	case 2:
		return prefix + "short"
	case 4:
		return prefix + "int"
	case 8:
		return prefix + "int64"
	default:
		panic("bad int length")
	}
}

func (fixedPointManagerType) goTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	prefix := ""
	if !attr.signed {
		prefix = "u"
	}
	switch attr.length {
	case 1:
		return prefix + "int8"
	case 2:
		return prefix + "int16"
	case 4:
		return prefix + "int32"
	case 8:
		return prefix + "int64"
	default:
		panic("bad int length")
	}
}

func (fixedPointManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	var values interface{}
	switch attr.length {
	case 1:
		values = allocInt8s(bf, dimensions, attr.signed, nil)
	case 2:
		values = allocShorts(bf, dimensions, attr.endian, attr.signed, nil)
	case 4:
		values = allocInts(bf, dimensions, attr.endian, attr.signed, nil)
	case 8:
		values = allocInt64s(bf, dimensions, attr.endian, attr.signed, nil)
	default:
		fail(fmt.Sprintf("bad size fixed: %d (%v)", attr.length, attr))
	}
	return values // already converted
}

func (fixedPointManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	switch obj.objAttr.length {
	case 1:
		if undefinedFillValue {
			fv := math.MinInt8 + 1
			objFillValue = []byte{byte(fv)}
		}
	case 2:
		if undefinedFillValue {
			fv := int16(math.MinInt16 + 1)
			var bb bytes.Buffer
			err := binary.Write(&bb, obj.objAttr.endian, fv)
			thrower.ThrowIfError(err)
			objFillValue = bb.Bytes()
		}
	case 4:
		if undefinedFillValue {
			fv := int32(math.MinInt32 + 1)
			var bb bytes.Buffer
			err := binary.Write(&bb, obj.objAttr.endian, fv)
			thrower.ThrowIfError(err)
			objFillValue = bb.Bytes()
		}
	case 8:
		if undefinedFillValue {
			fv := int64(math.MinInt64 + 1)
			var bb bytes.Buffer
			err := binary.Write(&bb, obj.objAttr.endian, fv)
			thrower.ThrowIfError(err)
			objFillValue = bb.Bytes()
		}
	}
	return objFillValue
}

func (fixedPointManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("* fixed-point")
	// Same structure for all versions, no need to check
	byteOrder := bitFields & 0b1
	paddingType := (bitFields >> 1) & 0b11
	signed := (bitFields >> 3) & 0b1
	attr.signed = signed == 0b1
	logger.Infof("byteOrder=%d paddingType=%d, signed=%d", byteOrder, paddingType, signed)
	if byteOrder != 0 {
		attr.endian = binary.BigEndian
	} else {
		attr.endian = binary.LittleEndian
	}
	assertError(paddingType == 0, ErrFixedPoint,
		fmt.Sprintf("fixed point padding must be zero 0x%x", bitFields))
	logger.Info("len properties", bf.Rem())
	assert(bf.Rem() > 0, "properties should be here")
	bitOffset := read16(bf)
	bitPrecision := read16(bf)
	logger.Infof("bitOffset=%d bitPrecision=%d blen=%d", bitOffset, bitPrecision,
		bf.Count())
	assertError(bitOffset == 0, ErrFixedPoint, "bit offset must be zero")
	switch attr.length {
	case 1, 2, 4, 8:
		break
	default:
		thrower.Throw(ErrFixedPoint)
	}
	if df == nil {
		logger.Infof("no data")
		return
	}
	if df.Rem() >= int64(attr.length) {
		attr.df = newResetReaderSave(df, df.Rem())
	}
}

func allocInt8s(bf io.Reader, dimLengths []uint64, signed bool, cast reflect.Type) interface{} {
	if cast == nil {
		if signed {
			cast = reflect.TypeOf(int8(0))
		} else {
			cast = reflect.TypeOf(uint8(0))
		}
	}
	if len(dimLengths) == 0 {
		value := read8(bf)
		return reflect.ValueOf(value).Convert(cast).Interface()
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := reflect.MakeSlice(reflect.SliceOf(cast), int(thisDim), int(thisDim)).Interface()
		err := binary.Read(bf, binary.LittleEndian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(cast, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocInt8s(bf, dimLengths[1:], signed, cast)))
	}
	return vals.Interface()
}

func allocShorts(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder, signed bool,
	cast reflect.Type) interface{} {
	if cast == nil {
		if signed {
			cast = reflect.TypeOf(int16(0))
		} else {
			cast = reflect.TypeOf(uint16(0))
		}
	}
	if len(dimLengths) == 0 {
		var value uint16
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return reflect.ValueOf(value).Convert(cast).Interface()
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := reflect.MakeSlice(reflect.SliceOf(cast), int(thisDim), int(thisDim)).Interface()
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(cast, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocShorts(bf, dimLengths[1:], endian, signed,
			cast)))
	}
	return vals.Interface()
}

func allocInts(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder, signed bool,
	cast reflect.Type) interface{} {
	if cast == nil {
		if signed {
			cast = reflect.TypeOf(int32(0))
		} else {
			cast = reflect.TypeOf(uint32(0))
		}
	}
	if len(dimLengths) == 0 {
		var value uint32
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return reflect.ValueOf(value).Convert(cast).Interface()
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := reflect.MakeSlice(reflect.SliceOf(cast), int(thisDim), int(thisDim)).Interface()
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(cast, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocInts(bf, dimLengths[1:], endian, signed,
			cast)))
	}
	return vals.Interface()
}

func allocInt64s(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder, signed bool,
	cast reflect.Type) interface{} {
	if cast == nil {
		if signed {
			cast = reflect.TypeOf(int64(0))
		} else {
			cast = reflect.TypeOf(uint64(0))
		}
	}
	if len(dimLengths) == 0 {
		var value uint64
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return reflect.ValueOf(value).Convert(cast).Interface()
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := reflect.MakeSlice(reflect.SliceOf(cast), int(thisDim), int(thisDim)).Interface()
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(cast, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocInt64s(bf, dimLengths[1:], endian, signed,
			cast)))
	}
	return vals.Interface()
}
