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
	for i := 0; i < 5; i++ {
		buf.WriteByte(0)
	}
	for _, d := range dimensions {
		binary.Write(buf, binary.LittleEndian, d)
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
	buf.WriteByte(0x20) // LE, standard bit settings
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)
	binary.Write(buf, binary.LittleEndian, uint32(size))
	
	if size == 4 {
		binary.Write(buf, binary.LittleEndian, uint16(0))   // bit offset
		binary.Write(buf, binary.LittleEndian, uint16(32))  // precision
		buf.WriteByte(31) // exp location
		buf.WriteByte(8)  // exp size
		buf.WriteByte(0)  // mantissa location
		buf.WriteByte(23) // mantissa size
		binary.Write(buf, binary.LittleEndian, uint32(127)) // bias
	} else {
		binary.Write(buf, binary.LittleEndian, uint16(0))   // bit offset
		binary.Write(buf, binary.LittleEndian, uint16(64))  // precision
		buf.WriteByte(63) // exp location
		buf.WriteByte(11) // exp size
		buf.WriteByte(0)  // mantissa location
		buf.WriteByte(52) // mantissa size
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
	if s, ok := val.(string); ok {
		buf.Write([]byte(s))
		buf.WriteByte(0)
	} else {
		binary.Write(buf, binary.LittleEndian, val)
	}
	
	return h5Message{mType: 12, data: buf.Bytes()}
}

func (hw *HDF5Writer) buildDatatypeMessage(val interface{}) []byte {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Len() == 0 {
			return buildFixedPointDatatype(4, true)
		}
		rv = rv.Index(0)
	}
	
	switch rv.Kind() {
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
		return buildStringDatatype(len(val.(string)) + 1)
	}
	return buildFixedPointDatatype(4, true)
}
