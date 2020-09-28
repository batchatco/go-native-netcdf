package hdf5

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/batchatco/go-thrower"
)

type referenceManagerType struct{}

var (
	referenceManager             = referenceManagerType{}
	_                typeManager = referenceManager
)

func (referenceManagerType) TypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	return "uint64" // reference same as uint64
}

func (referenceManagerType) GoTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	return "uint64" // reference same as uint64
}

func (referenceManagerType) Alloc(h5 *HDF5, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	return h5.allocReferences(bf, dimensions) // already converted
}

func (referenceManagerType) FillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (referenceManagerType) Parse(h5 *HDF5, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("* reference")
	checkVal(1, attr.dtversion, "Only support version 1 of reference")
	rType := bitFields & 0b1111
	switch rType {
	case 0:
		break
	case 1:
		break
	default:
		assert(df == nil, "references can't be attributes")
		maybeFail(fmt.Sprintf("invalid rtype value: %#b dtlength=%v", rType, attr.length))
		return
	}
	logger.Info("* rtype=object")
	warnAssert((bitFields & ^uint32(0b1111)) == 0, "reserved must be zero")
	if df == nil {
		logger.Infof("no data")
		return
	}
	if !allowReferences {
		assert(df == nil, "references can't be attributes")
		logger.Infof("References ignored")
		thrower.Throw(ErrReference)
	}
	if df.Rem() >= int64(attr.length) {
		attr.df = newResetReaderSave(df, df.Rem())
	}
}

func (h5 *HDF5) allocReferences(bf io.Reader, dimLengths []uint64) interface{} {
	if len(dimLengths) == 0 {
		var addr uint64
		err := binary.Read(bf, binary.LittleEndian, &addr)
		thrower.ThrowIfError(err)
		logger.Infof("Reference addr 0x%x", addr)
		return addr
	}

	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]uint64, thisDim)
		for i := range values {
			var addr uint64
			err := binary.Read(bf, binary.LittleEndian, &addr)
			thrower.ThrowIfError(err)
			logger.Infof("Reference addr[%d] 0x%x", i, addr)
			values[i] = addr
		}
		return values
	}
	vals := makeSlices(reflect.TypeOf(uint64(0)), dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(h5.allocReferences(bf, dimLengths[1:])))
	}
	return vals.Interface()
}
