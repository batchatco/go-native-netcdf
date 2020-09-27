package hdf5

import (
	"io"
)

type arrayManagerType struct {
	typeManager
}

var arrayManager = arrayManagerType{}
var _ typeManager = arrayManager

func (arrayManagerType) Alloc(h5 *HDF5, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	logger.Info("orig dimensions=", attr.dimensions)
	logger.Info("Array length=", attr.length)
	logger.Info("Array dimensions=", dimensions)
	arrayAttr := attr.children[0]
	logger.Info("child dimensions=", arrayAttr.dimensions)
	logger.Info("childlength=", arrayAttr.length)
	newDimensions := append(dimensions, arrayAttr.dimensions...)
	arrayAttr.dimensions = newDimensions
	logger.Info("new dimensions=", newDimensions)
	cbf := bf.(remReader)
	logger.Info(cbf.Count(), "child length", arrayAttr.length)
	logger.Info(cbf.Count(), "array", "class", arrayAttr.class)
	return h5.getDataAttr(cbf, *arrayAttr)
}

func (arrayManagerType) FillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (arrayManagerType) Parse(h5 *HDF5, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("Array")
	dimensionality := read8(bf)
	logger.Info("dimensionality", dimensionality)
	switch attr.dtversion {
	case dtversionStandard, dtversionArray:
		checkZeroes(bf, 3)
	}
	dimensions := make([]uint64, dimensionality)
	for i := 0; i < int(dimensionality); i++ {
		dimensions[i] = uint64(read32(bf))
		logger.Info("dim=", dimensions[i])
	}
	logger.Info("dimensions=", dimensions)
	if attr.dtversion < 3 {
		for i := 0; i < int(dimensionality); i++ {
			perm := read32(bf)
			logger.Info("perm=", perm)
		}
	}
	var arrayAttr attribute
	h5.printDatatype(bf, nil, 0, &arrayAttr)
	arrayAttr.dimensions = dimensions
	attr.children = append(attr.children, &arrayAttr)
	if df != nil && df.Rem() > 0 {
		logger.Info("Using an array in an attribute")
		attr.df = newResetReaderSave(df, df.Rem())
	}
}
