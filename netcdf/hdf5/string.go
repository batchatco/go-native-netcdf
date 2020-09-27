package hdf5

import (
	"io"
	"reflect"
)

type stringManagerType struct {
	typeManager
}

var stringManager = stringManagerType{}
var _ typeManager = stringManager

func (stringManagerType) TypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	return "string"
}

func (stringManagerType) GoTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	return "string"
}

func (stringManagerType) Alloc(h5 *HDF5, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	logger.Info("regular string", len(dimensions), "dtlen=", attr.length)
	return allocRegularStrings(bf, dimensions, attr.length) // already converted
}

func (stringManagerType) FillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	// return all zeros to get zero lengths
	return []byte{0}
}

func (stringManagerType) Parse(h5 *HDF5, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	checkVal(1, attr.dtversion, "Only support version 1 of string")
	logger.Info("string")
	padding := bitFields & 0b1111
	set := (bitFields >> 3) & 0b1111
	if df == nil {
		logger.Infof("no data")
		return
	}
	b := make([]byte, df.Rem())
	read(df, b)
	logger.Infof("* string padding=%d set=%d b[%s]=%s", padding, set,
		attr.name, getString(b))
	attr.value = getString(b)
}

// Regular strings are fixed length, as opposed to variable length ones
func allocRegularStrings(bf io.Reader, dimLengths []uint64, dtlen uint32) interface{} {
	if len(dimLengths) == 0 {
		b := make([]byte, dtlen)
		read(bf, b)
		return getString(b)
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		b := make([]byte, thisDim)
		read(bf, b)
		return getString(b)
	}
	vals := makeStringSlices(dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocRegularStrings(bf, dimLengths[1:], dtlen)))
	}
	return vals.Interface()
}
