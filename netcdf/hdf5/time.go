package hdf5

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

type timeManagerType struct{}

var (
	timeManager             = timeManagerType{}
	_           typeManager = timeManager
	timeOnce                sync.Once
)

func (timeManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	timeOnce.Do(func() {
		logger.Warn("HDF5 time type is obsolete and ignored")
	})
	return ""
}

func (timeManagerType) goTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	timeOnce.Do(func() {
		logger.Warn("HDF5 time type is obsolete and ignored")
	})
	return ""
}

func (timeManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute, dimensions []uint64) any {
	timeOnce.Do(func() {
		logger.Warn("HDF5 time type is obsolete and ignored")
	})
	return nil
}

func (timeManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (timeManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	// This is disabled by default. Time is an obsolete type.
	timeOnce.Do(func() {
		logger.Warn("HDF5 time type is obsolete and ignored")
	})
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
		util.MustRead(bf, endian, &bp)
		logger.Info("time bit precision=", bp)
		if df.Rem() > 0 {
			fail("time")
		}
	} else {
		// skip the data
		if df != nil {
			skip(df, df.Rem())
		}
	}
}
