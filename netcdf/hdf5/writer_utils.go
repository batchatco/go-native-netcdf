package hdf5

import (
	"bytes"
	"reflect"

	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

func buildDataspaceMessage(dimensions []uint64) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 1) // version
	util.MustWriteByte(buf, byte(len(dimensions)))
	util.MustWriteByte(buf, 0) // flags

	if len(dimensions) == 0 {
		util.MustWriteByte(buf, 0) // type scalar
		for range 4 {
			util.MustWriteByte(buf, 0)
		}
	} else {
		util.MustWriteByte(buf, 0) // Reserved
		for range 4 {
			util.MustWriteByte(buf, 0)
		}
		for _, d := range dimensions {
			util.MustWriteLE(buf, d)
		}
	}
	return buf.Bytes()
}

func buildFixedPointDatatype(size int, signed bool) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x10) // version 1, class 0

	var b1 byte
	b1 = 0x00 // bit 0 = 0 (LE)
	if signed {
		b1 |= 0x08 // bit 3 = 1 (signed)
	}
	util.MustWriteByte(buf, b1)
	util.MustWriteByte(buf, 0) // b2
	util.MustWriteByte(buf, 0) // b3

	util.MustWriteLE(buf, uint32(size))

	util.MustWriteLE(buf, uint16(0)) // bit offset

	util.MustWriteLE(buf, uint16(size*8)) // precision

	return buf.Bytes()
}

func buildFloatingPointDatatype(size int) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x11) // version 1, class 1

	var b1, b2, b3 byte
	b1 = 0x20 // LE, mantissa norm = 2
	if size == 4 {
		b2 = 31 // sign location 31
	} else {
		b2 = 63 // sign location 63
	}
	util.MustWriteByte(buf, b1)
	util.MustWriteByte(buf, b2)
	util.MustWriteByte(buf, b3)

	util.MustWriteLE(buf, uint32(size))

	if size == 4 {
		util.MustWriteLE(buf, uint16(0))   // bit offset
		util.MustWriteLE(buf, uint16(32))  // precision
		util.MustWriteByte(buf, 23)        // exponent location
		util.MustWriteByte(buf, 8)         // exponent size
		util.MustWriteByte(buf, 0)         // mantissa location
		util.MustWriteByte(buf, 23)        // mantissa size
		util.MustWriteLE(buf, uint32(127)) // bias
	} else {
		util.MustWriteLE(buf, uint16(0))    // bit offset
		util.MustWriteLE(buf, uint16(64))   // precision
		util.MustWriteByte(buf, 52)         // exponent location
		util.MustWriteByte(buf, 11)         // exponent size
		util.MustWriteByte(buf, 0)          // mantissa location
		util.MustWriteByte(buf, 52)         // mantissa size
		util.MustWriteLE(buf, uint32(1023)) // bias
	}

	return buf.Bytes()
}

func buildStringDatatype(size int) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x13) // version 1, class 3 (string)
	util.MustWriteByte(buf, 0x00) // null-terminated, ASCII
	util.MustWriteByte(buf, 0x00)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteLE(buf, uint32(size))
	return buf.Bytes()
}

func (hw *HDF5Writer) buildAttributeMessage(name string, val any) h5Message {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 1) // version
	util.MustWriteByte(buf, 0) // reserved

	nameBytes := append([]byte(name), 0)
	util.MustWriteLE(buf, uint16(len(nameBytes)))

	dtMsg := hw.buildDatatypeMessage(val)
	util.MustWriteLE(buf, uint16(len(dtMsg)))

	dims := hw.getDimensions(val)
	dsMsg := buildDataspaceMessage(dims)
	util.MustWriteLE(buf, uint16(len(dsMsg)))
	writePadded := func(b []byte) {
		util.MustWriteRaw(buf, b)
		for (buf.Len() % 8) != 0 {
			util.MustWriteByte(buf, 0)
		}
	}

	writePadded(nameBytes)
	writePadded(dtMsg)
	writePadded(dsMsg)

	// Value
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	maxLen := 0
	if !hw.shouldUseVLen(rv) {
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.String {
			findMaxLen(rv, &maxLen)
			maxLen++ // include null terminator
		}
	}
	hw.writeAttributeDataRecursive(buf, rv, maxLen)

	return h5Message{mType: 12, data: buf.Bytes()}
}

func (hw *HDF5Writer) writeAttributeDataRecursive(buf *bytes.Buffer, rv reflect.Value, maxLen int) {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := range rv.Len() {
			hw.writeAttributeDataRecursive(buf, rv.Index(i), maxLen)
		}
		return
	}
	if rv.Kind() == reflect.String {
		if maxLen > 0 {
			str := rv.String()
			util.MustWriteRaw(buf, []byte(str))
			// Pad to maxLen
			for i := len(str); i < maxLen; i++ {
				util.MustWriteByte(buf, 0)
			}
		} else {
			// VLen string
			str := rv.String()
			idx := hw.heap.indices[str]
			util.MustWriteLE(buf, uint32(len(str)))
			util.MustWriteLE(buf, hw.heap.addr)
			util.MustWriteLE(buf, idx)
		}
		return
	}
	util.MustWriteLE(buf, rv.Interface())
}

func (hw *HDF5Writer) buildDatatypeMessage(val any) []byte {
	rv := reflect.ValueOf(val)
	t := rv.Type()
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Int8:
		return buildFixedPointDatatype(1, true)
	case reflect.Uint8:
		return buildFixedPointDatatype(1, false)
	case reflect.Int16:
		return buildFixedPointDatatype(2, true)
	case reflect.Uint16:
		return buildFixedPointDatatype(2, false)
	case reflect.Int32:
		return buildFixedPointDatatype(4, true)
	case reflect.Uint32:
		return buildFixedPointDatatype(4, false)
	case reflect.Int64:
		return buildFixedPointDatatype(8, true)
	case reflect.Uint64:
		return buildFixedPointDatatype(8, false)
	case reflect.Float32:
		return buildFloatingPointDatatype(4)
	case reflect.Float64:
		return buildFloatingPointDatatype(8)
	case reflect.String:
		if hw.shouldUseVLen(rv) {
			return hw.buildVLenStringDatatype()
		}
		maxLen := 0
		for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
			rv = rv.Elem()
		}
		findMaxLen(rv, &maxLen)
		return buildStringDatatype(maxLen + 1)
	}
	return buildFixedPointDatatype(4, true)
}

func (hw *HDF5Writer) buildVLenStringDatatype() []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x19) // version 1, class 9
	util.MustWriteByte(buf, 0x01) // type=1 (string), padding=0, cset=0
	util.MustWriteByte(buf, 0x00)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteLE(buf, uint32(16)) // seq length

	baseType := buildStringDatatype(1)
	util.MustWriteRaw(buf, baseType)
	return buf.Bytes()
}

func findMaxLen(rv reflect.Value, maxLen *int) {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := range rv.Len() {
			findMaxLen(rv.Index(i), maxLen)
		}
		return
	}
	if rv.Kind() == reflect.String {
		l := len(rv.String())
		if l > *maxLen {
			*maxLen = l
		}
	}
}
