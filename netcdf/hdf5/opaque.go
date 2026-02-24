package hdf5

import (
	"fmt"
	"io"
	"reflect"
)

type opaqueManagerType struct{}

type opaque []byte

var (
	opaqueManager             = opaqueManagerType{}
	_             typeManager = opaqueManager
)

func (opaqueManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	signature := fmt.Sprintf("opaque(%d)", attr.length)
	namedType := sh.findSignature(signature, name, origNames, cdlTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (opaqueManagerType) goTypeString(sh sigHelper, typeName string, attr *attribute, origNames map[string]bool) string {
	signature := fmt.Sprintf("[%d]uint8", attr.length)
	namedType := sh.findSignature(signature, typeName, origNames, goTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (opaqueManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) any {
	cast := c.cast(*attr)
	return allocOpaque(bf, dimensions, attr.length, cast)
}

func (opaqueManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (opaqueManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	if bf.Rem() == 0 {
		logger.Info("No properties for opaque")
		return
	}
	plen := int(bf.Rem())
	tag := make([]byte, plen)
	// The tag is a user-defined ASCII string describing the opaque data (per the HDF5 spec).
	read(bf, tag)
	stringTag := getString(tag)
	logger.Info("tag=", stringTag)
	taglen := len(stringTag)
	for i := taglen; i < plen; i++ {
		checkVal(0, tag[i],
			fmt.Sprint("reserved byte should be zero: ", i))
	}
	if df != nil && df.Rem() >= int64(attr.length) {
		attr.df = newResetReaderSave(df, df.Rem())
	}
}

func allocOpaque(bf io.Reader, dimLengths []uint64, length uint32,
	cast reflect.Type) any {
	if len(dimLengths) == 0 {
		if cast != nil {
			b := reflect.New(cast)
			read(bf, b.Interface())
			return reflect.Indirect(b).Interface()
		}
		b := make([]byte, length)
		read(bf, b)
		return opaque(b)
	}
	thisDim := dimLengths[0]
	var ty reflect.Type
	if cast != nil {
		ty = cast
	} else {
		ty = reflect.TypeOf(opaque{})
	}
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		val := allocOpaque(bf, dimLengths[1:], length, cast)
		vals.Index(int(i)).Set(reflect.ValueOf(val))
	}
	return vals.Interface()
}
