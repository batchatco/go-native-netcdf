// Opens NetCDF files, regardless of type (CDF or HDF5)
package netcdf

import (
	"errors"
	"io"
	"os"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/cdf"
	"github.com/batchatco/go-native-netcdf/netcdf/hdf5"
)

const (
	magicCDF = 'C'
	magicHDF = 0x89
)

var ErrUnknown = errors.New("not a CDF or HDF5 file")

// Open opens a NetCDF4 file by name
func Open(fname string) (api.Group, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	kind, err := getKind(file)
	if err != nil {
		return nil, ErrUnknown
	}
	var g api.Group
	err = ErrUnknown
	switch kind {
	case magicCDF:
		g, err = cdf.Open(fname)
	case magicHDF:
		g, err = hdf5.Open(fname)
	}
	return g, err
}

// New is like Open, but takes an opened file instead of a filename.
// If New returns no error, it has taken ownership of the file.  Otherwise, it
// is up to the caller to close the file.
func New(file api.ReadSeekerCloser) (api.Group, error) {
	kind, err := getKind(file)
	if err != nil {
		return nil, ErrUnknown
	}
	var g api.Group
	err = ErrUnknown
	switch kind {
	case magicCDF:
		g, err = cdf.New(file)
	case magicHDF:
		g, err = hdf5.New(file)
	}
	return g, err
}

func getKind(file io.ReadSeeker) (byte, error) {
	var b [1]byte
	n, err := file.Read(b[:])
	if n == 0 {
		return 0, err
	}
	_, err = file.Seek(0, io.SeekStart)
	return b[0], err
}
