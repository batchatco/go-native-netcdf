package hdf5

import (
	"errors"
	"io"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

var ErrNotImplemented = errors.New("HDF5 is not implemented yet")

var (
	logger = util.NewLogger()
	log    = "don't use the log package" // prevents usage of standard log package
)

func Open(fname string) (nc api.Group, err error) {
	return nil, ErrNotImplemented
}

func New(file io.ReadSeeker) (nc api.Group, err error) {
	return nil, ErrNotImplemented
}

func SetLogLevel(level int) {
	logger.SetLogLevel(level)
}
