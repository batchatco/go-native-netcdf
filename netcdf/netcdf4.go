package netcdf

import (
	"os"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/cdf"
	"github.com/batchatco/go-native-netcdf/netcdf/hdf5"
)

func NewNetCDF4(fname string) (api.Group, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	var b [1]byte
	n, err := file.Read(b[:])
	file.Close()
	if n == 0 {
		return nil, err
	}

	if b[0] == 'C' {
		return cdf.NewCDF(fname)
	} else {
		return hdf5.NewHDF5(fname)
	}
}
