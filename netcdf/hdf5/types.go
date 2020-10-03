package hdf5

import (
	"fmt"
	"io"
	"reflect"
)

// Each type implements this interface.
type typeManager interface {
	parse(hr heapReader, c caster, attr *attribute, bitFields uint32, f remReader, d remReader)
	defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte
	alloc(hr heapReader, c caster, r io.Reader, attr *attribute, dimensions []uint64) interface{}
	cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string
	goTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string
}

type heapReader interface {
	readGlobalHeap(heapAddress uint64, index uint32) (remReader, uint64)
}

type caster interface {
	cast(attr attribute) reflect.Type
}

type sigHelper interface {
	findSignature(signature string, name string, origNames map[string]bool,
		printer printerType) string
}

type printerType func(class uint8, helper sigHelper, name string, attr *attribute,
	origNames map[string]bool) string

var dispatch = []typeManager{
	// 0-4
	fixedPointManager,
	floatingPointManager,
	timeManager,
	stringManager,
	bitfieldManager,
	// 5-9
	opaqueManager,
	compoundManager,
	referenceManager,
	enumManager,
	vlenManager,
	// 10
	arrayManager,
}

func getDispatch(class uint8) typeManager {
	if int(class) >= len(dispatch) {
		fail(fmt.Sprintf("Unknown class: %d", class))
	}
	return dispatch[class]
}

// parse is a wrapper around the table lookup of the type to get the interface
func parse(class uint8, hr heapReader, c caster, attr *attribute, bitFields uint32, f remReader, d remReader) {
	getDispatch(class).parse(hr, c, attr, bitFields, f, d)
}

// defaultFillValue is a wrapper around the table lookup of the type to get the interface
func defaultFillValue(class uint8, obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return getDispatch(class).defaultFillValue(obj, objFillValue, undefinedFillValue)
}

func alloc(class uint8, hr heapReader, c caster, r io.Reader, attr *attribute, dimensions []uint64) interface{} {
	return getDispatch(class).alloc(hr, c, r, attr, dimensions)
}

func cdlTypeString(class uint8, sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	return getDispatch(class).cdlTypeString(sh, name, attr, origNames)
}

func goTypeString(class uint8, sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	return getDispatch(class).goTypeString(sh, name, attr, origNames)
}

func printDatatype(hr heapReader, c caster, bf remReader, df remReader, objCount int64, attr *attribute) {
	assert(bf.Rem() >= 8, "short data")
	b0 := read8(bf)
	b1 := read8(bf)
	b2 := read8(bf)
	b3 := read8(bf)
	bitFields := uint32(b1) | (uint32(b2) << 8) | (uint32(b3) << 16)
	dtversion := (b0 >> 4) & 0b1111
	dtclass := b0 & 0b1111
	dtlength := read32(bf)
	logger.Infof("* length=%d dtlength=%d dtversion=%d class=%s flags=%s",
		bf.Rem(), dtlength,
		dtversion, typeNames[dtclass], binaryToString(uint64(bitFields)))
	switch dtversion {
	case dtversionStandard:
		logger.Info("Standard datatype")
	case dtversionArray:
		logger.Info("Array-encoded datatype")
	case dtversionPacked:
		logger.Info("VAX and/or packed datatype")
	case dtversionV4:
		if maxDTVersion == dtversionV4 {
			// allowed
			logger.Info("Undocumented datatype version 4")
			break
		}
		fallthrough
	default:
		fail(fmt.Sprint("Unknown datatype version: ", dtversion))
	}
	attr.dtversion = dtversion
	attr.class = dtclass
	attr.length = dtlength
	assert(attr.length != 0, "attr length can't be zero")
	parse(dtclass, hr, c, attr, bitFields, bf, df)
	if df != nil && df.Rem() > 0 {
		// It is normal for there to be extra data, not sure why yet.
		// It does not break any unit tests, so the extra data seems unnecessary.
		logger.Info("did not read all data", df.Rem(), typeNames[dtclass])
		skip(df, df.Rem())
	}
}
