package internal

import "github.com/batchatco/go-native-netcdf/netcdf/api"

type slice struct {
	getSlice func(begin, end int64) (interface{}, error)
	length   int64
	dimNames []string
	attrs    api.AttributeMap
}

func (sl *slice) GetSlice(begin, end int64) (interface{}, error) {
	return sl.getSlice(begin, end)
}

func (sl *slice) Values() (interface{}, error) {
	return sl.getSlice(0, sl.length)
}

func (sl *slice) Len() int64 {
	return sl.length
}

func (sl *slice) Attributes() api.AttributeMap {
	return sl.attrs
}

func (sl *slice) Dimensions() []string {
	return sl.dimNames
}

func NewSlicer(getSlice func(begin, end int64) (interface{}, error),
	length int64, dimNames []string, attributes api.AttributeMap) api.VarGetter {
	return &slice{
		getSlice: getSlice,
		length:   length,
		dimNames: dimNames,
		attrs:    attributes}
}
