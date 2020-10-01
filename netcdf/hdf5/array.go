package hdf5

import (
	"fmt"
	"io"
	"strings"
)

type arrayManagerType struct{}

var (
	arrayManager             = arrayManagerType{}
	_            typeManager = arrayManager
)

func (arrayManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	arrayAttr := attr.children[0]
	ty := cdlTypeString(arrayAttr.class, sh, name, arrayAttr, origNames)
	assert(ty != "", "unable to parse array attr")
	dStr := make([]string, len(arrayAttr.dimensions))
	for i, d := range arrayAttr.dimensions {
		dStr[i] = fmt.Sprintf("%d", d)
	}
	dims := strings.Join(dStr, ",")
	signature := fmt.Sprintf("%s(%s)", ty, dims)
	namedType := sh.findSignature(signature, name, origNames, cdlTypeString)
	assert(namedType == "", "arrays are not named types")
	return signature
}

func (arrayManagerType) goTypeString(sh sigHelper, typeName string, attr *attribute, origNames map[string]bool) string {
	arrayAttr := attr.children[0]
	ty := goTypeString(arrayAttr.class, sh, typeName, arrayAttr, origNames)
	assert(ty != "", "unable to parse array attr")
	dStr := make([]string, len(arrayAttr.dimensions))
	for i, d := range arrayAttr.dimensions {
		dStr[i] = fmt.Sprintf("[%d]", d)
	}
	dims := strings.Join(dStr, "")
	signature := fmt.Sprintf("%s%s", dims, ty)
	namedType := sh.findSignature(signature, typeName, origNames, goTypeString)
	assert(namedType == "", "arrays are not named types")
	return signature
}

func (arrayManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
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
	return getDataAttr(hr, c, cbf, *arrayAttr)
}

func (arrayManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (arrayManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
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
	printDatatype(hr, c, bf, nil, 0, &arrayAttr)
	arrayAttr.dimensions = dimensions
	attr.children = append(attr.children, &arrayAttr)
	if df != nil && df.Rem() > 0 {
		logger.Info("Using an array in an attribute")
		attr.df = newResetReaderSave(df, df.Rem())
	}
}
