package hdf5

import (
	"io"
	"reflect"
)

type stringManagerType struct{}

var (
	stringManager             = stringManagerType{}
	_             typeManager = stringManager
)

func (stringManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	return "string"
}

func (stringManagerType) goTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	return "string"
}

func (stringManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	logger.Info("regular string", len(dimensions), "dtlen=", attr.length)
	return allocRegularStrings(bf, dimensions, attr.length) // already converted
}

func (stringManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	// return all zeros to get zero lengths
	return []byte{0}
}

func (stringManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
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
		// NetCDF convention: 1D character arrays are returned as a single concatenated string.
		// However, HDF5 strings can also be arrays of strings. 
		// If dtlen is 1, it's definitely a character array. 
		// If dtlen > 1, it's an array of strings.
		if dtlen == 1 {
			b := make([]byte, thisDim)
			read(bf, b)
			return getString(b)
		}
		values := make([]string, thisDim)
		for i := uint64(0); i < thisDim; i++ {
			b := make([]byte, dtlen)
			read(bf, b)
			values[i] = getString(b)
		}
		return values
	}
	ty := reflect.TypeOf("")
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocRegularStrings(bf, dimLengths[1:], dtlen)))
	}
	return vals.Interface()
}
