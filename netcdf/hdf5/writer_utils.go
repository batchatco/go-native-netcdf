package hdf5

import (
	"bytes"
	"encoding/binary"
	"reflect"

	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
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

func buildFixedPointDatatype(size int, signed bool, order binary.ByteOrder) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x10) // version 1, class 0

	var b1 byte
	if order == binary.BigEndian {
		b1 = 0x01 // bit 0 = 1 (BE)
	}
	if signed {
		b1 |= 0x08 // bit 3 = 1 (signed)
	}
	util.MustWriteByte(buf, b1)
	util.MustWriteByte(buf, 0) // b2
	util.MustWriteByte(buf, 0) // b3

	util.MustWriteLE(buf, uint32(size))
	util.MustWriteLE(buf, uint16(0))      // bit offset
	util.MustWriteLE(buf, uint16(size*8)) // precision

	return buf.Bytes()
}

func buildFloatingPointDatatype(size int, order binary.ByteOrder) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x11) // version 1, class 1

	var b1, b2, b3 byte
	b1 = 0x20 // mantissa norm = 2
	if order == binary.BigEndian {
		b1 |= 0x01 // bit 0 = 1 (BE)
	}
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

// calcFieldSize computes the byte size of a value as stored in a compound field.
func calcFieldSize(val any) int {
	switch v := val.(type) {
	case int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int64, uint64, float64:
		return 8
	case string:
		return 16 // vlen string descriptor
	case compound:
		size := 0
		for _, f := range v {
			size += calcFieldSize(f.Val)
		}
		return size
	case opaque:
		return len(v)
	case enumerated:
		// Size of the underlying scalar
		rv := reflect.ValueOf(v.values)
		for rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			if rv.Len() > 0 {
				rv = rv.Index(0)
			} else {
				rv = reflect.Zero(rv.Type().Elem())
			}
		}
		return calcFieldSize(rv.Interface())
	}
	// Slice = vlen descriptor
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		return 16
	}
	thrower.Throw(ErrUnsupportedType)
	return 0
}

// writeEnc writes a 1, 2, or 4 byte encoded integer for compound byte offsets.
func writeEnc(buf *bytes.Buffer, val uint32, totalSize uint32) {
	switch {
	case totalSize < 256:
		util.MustWriteByte(buf, byte(val))
	case totalSize < 65536:
		util.MustWriteLE(buf, uint16(val))
	default:
		util.MustWriteLE(buf, val)
	}
}

// navigateToFirst descends through slice levels to reach the first element
// at the given depth. Returns a zero value if any slice along the way is empty.
func navigateToFirst(rv reflect.Value, depth int) reflect.Value {
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	for range depth {
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			break
		}
		if rv.Len() == 0 {
			return reflect.Zero(rv.Type().Elem())
		}
		rv = rv.Index(0)
	}
	return rv
}

func buildOpaqueDatatype(size int) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x15) // version 1, class 5 (opaque)
	util.MustWriteByte(buf, 0x00) // tag length = 0
	util.MustWriteByte(buf, 0x00)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteLE(buf, uint32(size))
	return buf.Bytes()
}

func (hw *HDF5Writer) buildCompoundDatatype(val compound) []byte {
	buf := new(bytes.Buffer)

	// Compute total compound size
	totalSize := 0
	for _, f := range val {
		totalSize += calcFieldSize(f.Val)
	}

	// Header: version 3, class 6
	nMembers := len(val)
	util.MustWriteByte(buf, 0x36)
	util.MustWriteByte(buf, byte(nMembers&0xFF))
	util.MustWriteByte(buf, byte((nMembers>>8)&0xFF))
	util.MustWriteByte(buf, byte((nMembers>>16)&0xFF))
	util.MustWriteLE(buf, uint32(totalSize))

	// Members
	offset := uint32(0)
	for _, f := range val {
		// Name (null-terminated, no padding for version 3)
		util.MustWriteRaw(buf, append([]byte(f.Name), 0))
		// Byte offset (encoded)
		writeEnc(buf, offset, uint32(totalSize))
		// Member datatype
		var dt []byte
		if _, ok := f.Val.(string); ok {
			// Strings in compounds are always vlen
			dt = hw.buildVLenStringDatatype()
		} else {
			// For compound members, nDims=0: any remaining slice is vlen
			dt = hw.buildDatatypeMessage(f.Val, 0)
		}
		util.MustWriteRaw(buf, dt)
		offset += uint32(calcFieldSize(f.Val))
	}

	return buf.Bytes()
}

func (hw *HDF5Writer) buildEnumDatatype(e enumerated) []byte {
	buf := new(bytes.Buffer)

	// Determine base integer type from the inner values
	rv := reflect.ValueOf(e.values)
	for rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Len() > 0 {
			rv = rv.Index(0)
		} else {
			rv = reflect.Zero(rv.Type().Elem())
		}
	}

	var baseSize int
	var signed bool
	switch rv.Interface().(type) {
	case int8:
		baseSize, signed = 1, true
	case uint8:
		baseSize, signed = 1, false
	case int16:
		baseSize, signed = 2, true
	case uint16:
		baseSize, signed = 2, false
	case int32:
		baseSize, signed = 4, true
	case uint32:
		baseSize, signed = 4, false
	case int64:
		baseSize, signed = 8, true
	case uint64:
		baseSize, signed = 8, false
	}

	nMembers := len(e.names)

	// Header: version 3, class 8
	util.MustWriteByte(buf, 0x38)
	util.MustWriteByte(buf, byte(nMembers&0xFF))
	util.MustWriteByte(buf, byte((nMembers>>8)&0xFF))
	util.MustWriteByte(buf, byte((nMembers>>16)&0xFF))
	util.MustWriteLE(buf, uint32(baseSize))

	// Base type (fixed-point)
	baseDt := buildFixedPointDatatype(baseSize, signed, hw.byteOrder)
	util.MustWriteRaw(buf, baseDt)

	// Member names (null-terminated, no padding for version 3)
	for _, name := range e.names {
		util.MustWriteRaw(buf, append([]byte(name), 0))
	}

	// Member values
	for _, val := range e.memberVals {
		util.MustWrite(buf, hw.byteOrder, val)
	}

	return buf.Bytes()
}

func (hw *HDF5Writer) buildVlenSequenceDatatype(rv reflect.Value) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x19) // version 1, class 9
	util.MustWriteByte(buf, 0x00) // type=0 (sequence), padding=0, cset=0
	util.MustWriteByte(buf, 0x00)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteLE(buf, uint32(16)) // vlen descriptor size

	// Build element datatype: all nesting within the element is array dimensions
	var elemVal any
	if rv.Len() > 0 {
		elemVal = rv.Index(0).Interface()
	} else {
		elemVal = reflect.Zero(rv.Type().Elem()).Interface()
	}
	elemRv := reflect.ValueOf(elemVal)
	for elemRv.Kind() == reflect.Pointer || elemRv.Kind() == reflect.Interface {
		elemRv = elemRv.Elem()
	}
	allDims := getDimensionsRecursive(elemRv)
	elemDt := hw.buildDatatypeMessage(elemVal, len(allDims))
	util.MustWriteRaw(buf, elemDt)
	return buf.Bytes()
}

func (hw *HDF5Writer) buildAttributeMessage(name string, val any) h5Message {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 1) // version
	util.MustWriteByte(buf, 0) // reserved

	nameBytes := append([]byte(name), 0)
	util.MustWriteLE(buf, uint16(len(nameBytes)))

	dims, nDims := hw.getAttributeDimensions(val)
	dtMsg := hw.buildDatatypeMessage(val, nDims)
	util.MustWriteLE(buf, uint16(len(dtMsg)))

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
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	maxLen := 0
	if !hw.shouldUseVLen(rv) {
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.String {
			findMaxLen(rv, &maxLen)
			maxLen++ // include null terminator
		}
	}
	hw.writeAttributeDataRecursive(buf, rv, maxLen, nDims)

	return h5Message{mType: 12, data: buf.Bytes()}
}

func (hw *HDF5Writer) writeAttributeDataRecursive(buf *bytes.Buffer, rv reflect.Value, maxLen int, dimRemaining int) {
	// Handle special types before general slice processing
	switch v := rv.Interface().(type) {
	case compound:
		hw.writeCompoundValue(buf, v)
		return
	case opaque:
		util.MustWriteRaw(buf, []byte(v))
		return
	case enumerated:
		hw.writeAttributeDataRecursive(buf, reflect.ValueOf(v.values), maxLen, dimRemaining)
		return
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// When dimRemaining == 0, this slice is vlen: write descriptor
		if dimRemaining <= 0 {
			idx := hw.vlenHeapIndices[hw.vlenPos]
			util.MustWriteLE(buf, uint32(rv.Len()))
			util.MustWriteLE(buf, hw.heap.addr)
			util.MustWriteLE(buf, idx)
			hw.vlenPos++
			return
		}
		for i := range rv.Len() {
			hw.writeAttributeDataRecursive(buf, rv.Index(i), maxLen, dimRemaining-1)
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
	switch rv.Kind() {
	case reflect.Int8, reflect.Uint8,
		reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32,
		reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		util.MustWrite(buf, hw.byteOrder, rv.Interface())
	default:
		thrower.Throw(ErrUnsupportedType)
	}
}

// writeCompoundValue writes all fields of a compound value.
func (hw *HDF5Writer) writeCompoundValue(buf *bytes.Buffer, val compound) {
	for _, f := range val {
		switch v := f.Val.(type) {
		case int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64:
			util.MustWrite(buf, hw.byteOrder, v)
		case string:
			// Vlen string descriptor
			idx := hw.heap.indices[v]
			util.MustWriteLE(buf, uint32(len(v)))
			util.MustWriteLE(buf, hw.heap.addr)
			util.MustWriteLE(buf, idx)
		case compound:
			hw.writeCompoundValue(buf, v)
		case opaque:
			util.MustWriteRaw(buf, []byte(v))
		case enumerated:
			util.MustWrite(buf, hw.byteOrder, v.values)
		default:
			// Vlen sequence descriptor
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Slice {
				idx := hw.vlenHeapIndices[hw.vlenPos]
				util.MustWriteLE(buf, uint32(rv.Len()))
				util.MustWriteLE(buf, hw.heap.addr)
				util.MustWriteLE(buf, idx)
				hw.vlenPos++
			} else {
				thrower.Throw(ErrUnsupportedType)
			}
		}
	}
}

func (hw *HDF5Writer) buildDatatypeMessage(val any, nDims int) []byte {
	// Handle enumerated (struct wrapper) at top level
	if e, ok := val.(enumerated); ok {
		return hw.buildEnumDatatype(e)
	}

	rv := reflect.ValueOf(val)
	origRv := rv // keep original for string vlen detection
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}

	// Navigate through nDims array dimensions to find element type
	t := rv.Type()
	dimsStripped := 0
	for dimsStripped < nDims && (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
		if t == reflect.TypeOf(compound{}) || t == reflect.TypeOf(opaque{}) {
			break
		}
		t = t.Elem()
		dimsStripped++
	}

	// Check for special types at element level
	if t == reflect.TypeOf(enumerated{}) {
		elem := navigateToFirst(rv, dimsStripped)
		return hw.buildEnumDatatype(elem.Interface().(enumerated))
	}
	if t == reflect.TypeOf(compound{}) {
		elem := navigateToFirst(rv, dimsStripped)
		return hw.buildCompoundDatatype(elem.Interface().(compound))
	}
	if t == reflect.TypeOf(opaque{}) {
		elem := navigateToFirst(rv, dimsStripped)
		o := elem.Interface().(opaque)
		return buildOpaqueDatatype(len(o))
	}

	// If still a slice after consuming declared dims, it's vlen
	if t.Kind() == reflect.Slice {
		elem := navigateToFirst(rv, dimsStripped)
		return hw.buildVlenSequenceDatatype(elem)
	}

	// Scalar types
	switch t.Kind() {
	case reflect.Int8:
		return buildFixedPointDatatype(1, true, hw.byteOrder)
	case reflect.Uint8:
		return buildFixedPointDatatype(1, false, hw.byteOrder)
	case reflect.Int16:
		return buildFixedPointDatatype(2, true, hw.byteOrder)
	case reflect.Uint16:
		return buildFixedPointDatatype(2, false, hw.byteOrder)
	case reflect.Int32:
		return buildFixedPointDatatype(4, true, hw.byteOrder)
	case reflect.Uint32:
		return buildFixedPointDatatype(4, false, hw.byteOrder)
	case reflect.Int64:
		return buildFixedPointDatatype(8, true, hw.byteOrder)
	case reflect.Uint64:
		return buildFixedPointDatatype(8, false, hw.byteOrder)
	case reflect.Float32:
		return buildFloatingPointDatatype(4, hw.byteOrder)
	case reflect.Float64:
		return buildFloatingPointDatatype(8, hw.byteOrder)
	case reflect.String:
		if hw.shouldUseVLen(origRv) {
			return hw.buildVLenStringDatatype()
		}
		for origRv.Kind() == reflect.Pointer || origRv.Kind() == reflect.Interface {
			origRv = origRv.Elem()
		}
		maxLen := 0
		findMaxLen(origRv, &maxLen)
		return buildStringDatatype(maxLen + 1)
	}
	thrower.Throw(ErrUnsupportedType)
	return nil
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

func buildReferenceDatatype() []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x17) // version 1, class 7 (reference)
	util.MustWriteByte(buf, 0x00) // object reference (type 0)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteLE(buf, uint32(8)) // 8 bytes for object reference
	return buf.Bytes()
}

func buildVLenReferenceDatatype() []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0x19) // version 1, class 9 (vlen)
	util.MustWriteByte(buf, 0x00) // type=0 (sequence), padding=0, cset=0
	util.MustWriteByte(buf, 0x00)
	util.MustWriteByte(buf, 0x00)
	util.MustWriteLE(buf, uint32(16)) // vlen descriptor size: uint32 + uint64 + uint32
	util.MustWriteRaw(buf, buildReferenceDatatype())
	return buf.Bytes()
}

// buildDimensionListAttribute builds the DIMENSION_LIST attribute message
// for a variable with the given dimension names.
func (hw *HDF5Writer) buildDimensionListAttribute(dimNames []string) *h5Message {
	// Check that all dimensions have addresses and heap entries
	for _, dname := range dimNames {
		if dname == "" {
			return nil
		}
		if _, ok := hw.dimAddrs[dname]; !ok {
			return nil
		}
	}

	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 1) // version
	util.MustWriteByte(buf, 0) // reserved

	nameBytes := append([]byte("DIMENSION_LIST"), 0)
	util.MustWriteLE(buf, uint16(len(nameBytes)))

	dtMsg := buildVLenReferenceDatatype()
	util.MustWriteLE(buf, uint16(len(dtMsg)))

	dsMsg := buildDataspaceMessage([]uint64{uint64(len(dimNames))})
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

	// Write VLEN descriptors: for each dimension, one reference
	for _, dname := range dimNames {
		heapIdx := hw.heap.indices["__dimref:"+dname]
		util.MustWriteLE(buf, uint32(1))       // length: 1 reference
		util.MustWriteLE(buf, hw.heap.addr)    // global heap address
		util.MustWriteLE(buf, heapIdx)         // global heap object index
	}

	msg := h5Message{mType: 12, data: buf.Bytes()}
	return &msg
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
