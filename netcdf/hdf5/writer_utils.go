package hdf5

import (
	"bytes"
	"encoding/binary"
	"reflect"
)

func buildDataspaceMessage(dimensions []uint64) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(1) // version
	buf.WriteByte(byte(len(dimensions)))
	buf.WriteByte(0) // flags
	
	if len(dimensions) == 0 {
		buf.WriteByte(0) // type scalar
		for i := 0; i < 4; i++ {
			buf.WriteByte(0)
		}
	} else {
		buf.WriteByte(0) // Reserved
		for i := 0; i < 4; i++ {
			buf.WriteByte(0)
		}
		for _, d := range dimensions {
			binary.Write(buf, binary.LittleEndian, d)
		}
	}
	return buf.Bytes()
}

func buildFixedPointDatatype(size int, signed bool) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x10) // version 1, class 0
	
	var b1 byte
	b1 = 0x00 // bit 0 = 0 (LE)
	if signed {
		b1 |= 0x08 // bit 3 = 1 (signed)
	}
	buf.WriteByte(b1)
	buf.WriteByte(0) // b2
	buf.WriteByte(0) // b3
	
	binary.Write(buf, binary.LittleEndian, uint32(size))
	binary.Write(buf, binary.LittleEndian, uint16(0))      // bit offset
	binary.Write(buf, binary.LittleEndian, uint16(size*8)) // precision
	
	return buf.Bytes()
}

func buildFloatingPointDatatype(size int) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x11) // version 1, class 1
	
	var b1, b2, b3 byte
	b1 = 0x20 // LE, mantissa norm = 2
	if size == 4 {
		b2 = 31 // sign location 31
	} else {
		b2 = 63 // sign location 63
	}
	buf.WriteByte(b1)
	buf.WriteByte(b2)
	buf.WriteByte(b3)
	
	binary.Write(buf, binary.LittleEndian, uint32(size))
	
	if size == 4 {
		binary.Write(buf, binary.LittleEndian, uint16(0))   // bit offset
		binary.Write(buf, binary.LittleEndian, uint16(32))  // precision
		buf.WriteByte(23)  // exponent location
		buf.WriteByte(8)   // exponent size
		buf.WriteByte(0)   // mantissa location
		buf.WriteByte(23)  // mantissa size
		binary.Write(buf, binary.LittleEndian, uint32(127)) // bias
	} else {
		binary.Write(buf, binary.LittleEndian, uint16(0))   // bit offset
		binary.Write(buf, binary.LittleEndian, uint16(64))  // precision
		buf.WriteByte(52)  // exponent location
		buf.WriteByte(11)  // exponent size
		buf.WriteByte(0)   // mantissa location
		buf.WriteByte(52)  // mantissa size
		binary.Write(buf, binary.LittleEndian, uint32(1023)) // bias
	}
	
	return buf.Bytes()
}

func buildStringDatatype(size int) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x13) // version 1, class 3 (string)
	buf.WriteByte(0x00) // null-terminated, ASCII
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)
	binary.Write(buf, binary.LittleEndian, uint32(size))
	return buf.Bytes()
}

func (hw *HDF5Writer) buildAttributeMessage(name string, val interface{}) h5Message {
	buf := new(bytes.Buffer)
	buf.WriteByte(1) // version
	buf.WriteByte(0) // reserved
	
	nameBytes := append([]byte(name), 0)
	binary.Write(buf, binary.LittleEndian, uint16(len(nameBytes)))
	
	dtMsg := hw.buildDatatypeMessage(val)
	binary.Write(buf, binary.LittleEndian, uint16(len(dtMsg)))
	
	dims := hw.getDimensions(val)
	dsMsg := buildDataspaceMessage(dims)
	binary.Write(buf, binary.LittleEndian, uint16(len(dsMsg)))
	
	writePadded := func(b []byte) {
		buf.Write(b)
		for (buf.Len() % 8) != 0 {
			buf.WriteByte(0)
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
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.String {
		findMaxLen(rv, &maxLen)
		maxLen++ // include null terminator
	}
	writeAttributeDataRecursive(buf, rv, maxLen)
	
	return h5Message{mType: 12, data: buf.Bytes()}
}

func writeAttributeDataRecursive(buf *bytes.Buffer, rv reflect.Value, maxLen int) {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := 0; i < rv.Len(); i++ {
			writeAttributeDataRecursive(buf, rv.Index(i), maxLen)
		}
		return
	}
	if rv.Kind() == reflect.String {
		str := rv.String()
		buf.Write([]byte(str))
		// Pad to maxLen
		for i := len(str); i < maxLen; i++ {
			buf.WriteByte(0)
		}
		return
	}
	binary.Write(buf, binary.LittleEndian, rv.Interface())
}

func (hw *HDF5Writer) buildDatatypeMessage(val interface{}) []byte {
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
		maxLen := 0
		for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
			rv = rv.Elem()
		}
		findMaxLen(rv, &maxLen)
		return buildStringDatatype(maxLen + 1)
	}
	return buildFixedPointDatatype(4, true)
}

func findMaxLen(rv reflect.Value, maxLen *int) {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := 0; i < rv.Len(); i++ {
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
