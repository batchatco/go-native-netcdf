package hdf5

import (
	"fmt"
	"io"

	"github.com/batchatco/go-thrower"
)

type enumManagerType struct {
	typeManager
}

var enumManager = enumManagerType{}
var _ typeManager = enumManager

func (enumManagerType) Alloc(h5 *HDF5, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	var values interface{}
	enumAttr := attr.children[0]
	cast := h5.cast(*enumAttr)
	switch enumAttr.class {
	case typeFixedPoint:
		switch enumAttr.length {
		case 1:
			values = allocInt8s(bf, dimensions, enumAttr.signed, cast)
		case 2:
			values = allocShorts(bf, dimensions, enumAttr.endian, enumAttr.signed, cast)
		case 4:
			values = allocInts(bf, dimensions, enumAttr.endian, enumAttr.signed, cast)
		case 8:
			values = allocInt64s(bf, dimensions, enumAttr.endian, enumAttr.signed, cast)
		default:
			fail(fmt.Sprintf("bad size enum fixed: %d", enumAttr.length))
		}
	case typeFloatingPoint:
		if floatEnums {
			// Floating point enums are not part of NetCDF.
			switch enumAttr.length {
			case 4:
				values = allocFloats(bf, dimensions, enumAttr.endian)
			case 8:
				values = allocDoubles(bf, dimensions, enumAttr.endian)
			default:
				fail(fmt.Sprintf("bad size enum float: %d", attr.length))
			}
			break
		}
		fallthrough
	default:
		fail(fmt.Sprint("can't handle this class: ", enumAttr.class))
	}
	if cast != nil {
		return values
	}
	return enumerated{values}
}

func (enumManagerType) FillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (enumManagerType) Parse(h5 *HDF5, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("blen begin", bf.Count())
	var enumAttr attribute
	h5.printDatatype(bf, nil, 0, &enumAttr)
	logger.Info("blen now", bf.Count())
	numberOfMembers := bitFields & 0b11111111
	logger.Info("number of members=", numberOfMembers)
	names := make([]string, numberOfMembers)
	padding := 7
	switch attr.dtversion {
	case dtversionStandard:
	case dtversionArray:
		break
	default:
		padding = 0
	}
	for i := uint32(0); i < numberOfMembers; i++ {
		name := readNullTerminatedName(bf, padding)
		names[i] = name
	}
	enumAttr.enumNames = names
	logger.Info("enum names:", names)
	assert(enumAttr.class == typeFixedPoint, "only fixed-point enums supported")
	switch enumAttr.length {
	case 1, 2, 4, 8:
		break
	default:
		thrower.Throw(ErrFixedPoint)
	}
	switch attr.length {
	case 1, 2, 4, 8:
		break
	default:
		thrower.Throw(ErrFixedPoint)
	}
	values := make([]interface{}, numberOfMembers)
	enumAttr.enumValues = values
	for i := uint32(0); i < numberOfMembers; i++ {
		values[i] = h5.getDataAttr(bf, enumAttr)
		switch values[i].(type) {
		case uint64, int64:
		case uint32, int32:
		case uint16, int16:
		case uint8, int8:
		default:
			// Other enumeration types are not supported in NetCDF
			fail("unknown enumeration type")
		}
	}
	enumAttr.enumValues = values

	logger.Info("enum values:", values)
	attr.children = []*attribute{&enumAttr}
	if df != nil && df.Rem() > 0 {
		// Read away some bytes
		attr.df = newResetReaderSave(df, df.Rem())
	}
}
