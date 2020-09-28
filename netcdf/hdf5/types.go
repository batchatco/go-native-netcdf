package hdf5

import (
	"fmt"
	"io"
)

// Each type implements this interface.
type typeManager interface {
	Parse(h5 *HDF5, attr *attribute, bitFields uint32, f remReader, d remReader)
	DefaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte
	Alloc(h5 *HDF5, r io.Reader, attr *attribute, dimensions []uint64) interface{}
	TypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string
	GoTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string
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

// Parse is a wrapper around the table lookup of the type to get the interface
func Parse(class uint8, h5 *HDF5, attr *attribute, bitFields uint32, f remReader, d remReader) {
	getDispatch(class).Parse(h5, attr, bitFields, f, d)
}

// DefaultFillValue is a wrapper around the table lookup of the type to get the interface
func DefaultFillValue(class uint8, obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return getDispatch(class).DefaultFillValue(obj, objFillValue, undefinedFillValue)
}

func Alloc(class uint8, h5 *HDF5, r io.Reader, attr *attribute, dimensions []uint64) interface{} {
	return getDispatch(class).Alloc(h5, r, attr, dimensions)
}

func TypeString(class uint8, h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	return getDispatch(class).TypeString(h5, name, attr, origNames)
}

func GoTypeString(class uint8, h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	return getDispatch(class).GoTypeString(h5, name, attr, origNames)
}
