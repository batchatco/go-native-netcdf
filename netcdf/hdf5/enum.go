package hdf5

import (
	"fmt"
	"io"
	"strings"

	"github.com/batchatco/go-thrower"
)

type enumManagerType struct{}

var (
	enumManager             = enumManagerType{}
	_           typeManager = enumManager
)

func (enumManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	assert(len(attr.children) == 1, "enum should have one child")
	enumAttr := attr.children[0]
	assert(len(enumAttr.children) == 0, "no recursion")
	ty := cdlTypeString(enumAttr.class, sh, name, enumAttr, origNames)
	assert(ty != "", "unable to parse enum attr")
	list := make([]string, len(enumAttr.enumNames))
	for i, name := range enumAttr.enumNames {
		list[i] = fmt.Sprintf("\t%s = %v", name, enumAttr.enumValues[i])
	}
	interior := strings.Join(list, ",\n")
	signature := fmt.Sprintf("%s enum {\n%s\n}", ty, interior)
	namedType := sh.findSignature(signature, name, origNames, cdlTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (enumManagerType) goTypeString(sh sigHelper, typeName string, attr *attribute, origNames map[string]bool) string {
	assert(len(attr.children) == 1, "enum should have one child")
	enumAttr := attr.children[0]
	assert(len(enumAttr.children) == 0, "no recursion")
	ty := goTypeString(enumAttr.class, sh, typeName, enumAttr, origNames)
	assert(ty != "", "unable to parse enum attr")
	list := make([]string, len(enumAttr.enumNames))
	for i, enumName := range enumAttr.enumNames {
		if i == 0 {
			list[i] = fmt.Sprintf("\t%s %s = %v", enumName, typeName, enumAttr.enumValues[i])
		} else {
			list[i] = fmt.Sprintf("\t%s = %v", enumName, enumAttr.enumValues[i])
		}
	}
	interior := strings.Join(list, "\n")
	signature := fmt.Sprintf("%s\nconst (\n%s\n)\n", ty, interior)
	namedType := sh.findSignature(signature, typeName, origNames, goTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (enumManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	var values interface{}
	enumAttr := attr.children[0]
	cast := c.cast(*enumAttr)
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

func (enumManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (enumManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("blen begin", bf.Count())
	var enumAttr attribute
	printDatatype(hr, c, bf, nil, 0, &enumAttr)
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
		values[i] = getDataAttr(hr, c, bf, enumAttr)
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
