package hdf5

import (
	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

type HDF5Writer struct {
}

func (hw *HDF5Writer) Close() error {
	return api.ErrUnsupported
}

func (hw *HDF5Writer) AddAttributes(attrs api.AttributeMap) error {
	return api.ErrUnsupported
}

func (hw *HDF5Writer) AddVar(name string, vr api.Variable) error {
	return api.ErrUnsupported
}

func (hw *HDF5Writer) CreateGroup(name string) (api.Writer, error) {
	return nil, api.ErrUnsupported
}

func OpenWriter(fileName string) (api.Writer, error) {
	return nil, api.ErrUnsupported
}
