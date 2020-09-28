package hdf5

import (
	"encoding/binary"
	"io"

	"github.com/batchatco/go-thrower"
)

type timeManagerType struct{}

var (
	timeManager             = timeManagerType{}
	_           typeManager = timeManager
)

func (timeManagerType) TypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	fail("time")
	return ""
}

func (timeManagerType) GoTypeString(h5 *HDF5, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	fail("time")
	return ""
}

func (timeManagerType) Alloc(h5 *HDF5, bf io.Reader, attr *attribute, dimensions []uint64) interface{} {
	fail("time")
	return nil
}

func (timeManagerType) DefaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	fail("time")
	return objFillValue
}

func (timeManagerType) Parse(h5 *HDF5, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	// This is disabled by default. Time is an obsolete type.
	if parseTime {
		logger.Info("time, len(data)=", df.Rem())
		var endian binary.ByteOrder
		if bitFields == 0 {
			endian = binary.LittleEndian
			logger.Info("time little-endian")
		} else {
			endian = binary.BigEndian
			logger.Infof("time big-endian")
		}
		var bp int16
		err := binary.Read(bf, endian, &bp)
		thrower.ThrowIfError(err)
		logger.Info("time bit precision=", bp)
		if df.Rem() > 0 {
			fail("time")
		}
	} else {
		logger.Fatal("time type is obsolete")
	}
}
