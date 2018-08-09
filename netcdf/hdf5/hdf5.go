package hdf5

import (
	"errors"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

var ErrNotImplemented = errors.New("HDF5 is not implemented yet")

func NewHDF5(fname string) (nc api.Group, err error) {
	return nil, ErrNotImplemented
}
