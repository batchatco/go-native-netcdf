package hdf5

import (
	"fmt"
	"io"
)

// Each type implements this interface.
type typeManager interface {
	parse(h5 *HDF5, attr *attribute, bitFields uint32, f remReader, d remReader)
	defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte
	alloc(h5 *HDF5, r io.Reader, attr *attribute, dimensions []uint64) interface{}
	cdlTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string
	goTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string
}

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
func parse(class uint8, h5 *HDF5, attr *attribute, bitFields uint32, f remReader, d remReader) {
	getDispatch(class).parse(h5, attr, bitFields, f, d)
}

// defaultFillValue is a wrapper around the table lookup of the type to get the interface
func defaultFillValue(class uint8, obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return getDispatch(class).defaultFillValue(obj, objFillValue, undefinedFillValue)
}

func alloc(class uint8, h5 *HDF5, r io.Reader, attr *attribute, dimensions []uint64) interface{} {
	return getDispatch(class).alloc(h5, r, attr, dimensions)
}

func cdlTypeString(class uint8, h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	return getDispatch(class).cdlTypeString(h5, name, attr, origNames)
}

func goTypeString(class uint8, h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	return getDispatch(class).goTypeString(h5, name, attr, origNames)
}
