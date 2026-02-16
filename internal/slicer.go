package internal

import (
	"reflect"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-thrower"
)

type slice struct {
	getSlice   func(begin, end int64) (any, error)
	getSliceMD func(begin, end []int64) (any, error)
	length     int64
	shape      []int64
	dimNames   []string
	attrs      api.AttributeMap
	cdlType    string
	goType     string
}

func (sl *slice) GetSlice(begin, end int64) (slice any, err error) {
	defer thrower.RecoverError(&err)
	slice, err = sl.getSlice(begin, end)
	return slice, err
}

func (sl *slice) GetSliceMD(begin, end []int64) (slice any, err error) {
	defer thrower.RecoverError(&err)
	slice, err = sl.getSliceMD(begin, end)
	return slice, err
}

func (sl *slice) Values() (values any, err error) {
	defer thrower.RecoverError(&err)
	values, err = sl.getSlice(0, sl.length)
	return values, err
}

func (sl *slice) Len() int64 {
	return sl.length
}

func (sl *slice) Shape() []int64 {
	return sl.shape
}

func (sl *slice) Attributes() api.AttributeMap {
	return sl.attrs
}

func (sl *slice) Dimensions() []string {
	return sl.dimNames
}

func (sl *slice) Type() string {
	return sl.cdlType
}

func (sl *slice) GoType() string {
	return sl.goType
}

func NewSlicer(getSlice func(begin, end int64) (any, error),
	getSliceMD func(begin, end []int64) (any, error),
	length int64, shape []int64, dimNames []string, attributes api.AttributeMap,
	cdlType string, goType string) api.VarGetter {
	return &slice{
		getSlice:   getSlice,
		getSliceMD: getSliceMD,
		length:     length,
		shape:      shape,
		dimNames:   dimNames,
		attrs:      attributes,
		cdlType:    cdlType,
		goType:     goType,
	}
}

func SliceMD(data any, dimensions []uint64, begin, end []int64) (any, error) {
	v := reflect.ValueOf(data)
	return sliceRecursive(v, dimensions, begin, end).Interface(), nil
}

func sliceRecursive(v reflect.Value, dims []uint64, begin, end []int64) reflect.Value {
	if len(dims) == 0 {
		return v
	}

	// If it's a slice, we slice it
	if v.Kind() == reflect.Slice {
		b := int(begin[0])
		e := int(end[0])
		sliced := v.Slice(b, e)

		if len(dims) > 1 {
			// For each element in the sliced dimension, recurse
			newLen := e - b
			newSlice := reflect.MakeSlice(sliced.Type(), newLen, newLen)
			for i := range newLen {
				res := sliceRecursive(sliced.Index(i), dims[1:], begin[1:], end[1:])
				newSlice.Index(i).Set(res)
			}
			return newSlice
		}
		return sliced
	}
	// For scalars or base types at the end of recursion
	return v
}
