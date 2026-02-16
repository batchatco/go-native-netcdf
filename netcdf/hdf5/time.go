package hdf5

import (
	"encoding/binary"
	"io"

	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

type timeManagerType struct{}

var (
	timeManager             = timeManagerType{}
	_           typeManager = timeManager
)

func (timeManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	fail("time")
	return ""
}

func (timeManagerType) goTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	// Not NetCDF
	fail("time")
	return ""
}

func (timeManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute, dimensions []uint64) any {
	fail("time")
	return nil
}

func (timeManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	fail("time")
	return objFillValue
}

func (timeManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
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
		util.MustRead(bf, endian, &bp)
		logger.Info("time bit precision=", bp)
		if df.Rem() > 0 {
			fail("time")
		}
	} else {
		logger.Fatal("time type is obsolete")
	}
}
