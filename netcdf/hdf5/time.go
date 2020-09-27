package hdf5

import (
	"io"

	"encoding/binary"

	"github.com/batchatco/go-thrower"
)

type timeManagerType struct {
	typeManager
}

var timeManager = timeManagerType{}
var _ typeManager = timeManager

func (timeManagerType) Alloc(h5 *HDF5, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	return nil
}

func (timeManagerType) FillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (timeManagerType) Parse(h5 *HDF5, attr *attribute, bitFields uint32, bf remReader, df remReader) {
		// uncomment the following to enable
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
			logger.Fatal("time code has never been executed before and does nothing")
		}
}
