package hdf5

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"

	"github.com/batchatco/go-native-netcdf/internal"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
)

var (
	ErrInvalidName = errors.New("invalid name")
)

type h5Var struct {
	name            string
	val             any
	dimensions      []string
	attributes      api.AttributeMap
	addr            uint64
	isDimScale      bool
	vlenHeapIndices []uint32            // heap indices for vlen elements
	attrVlenIndices map[string][]uint32 // per-attribute vlen heap indices
}

type h5Group struct {
	name            string
	groups          map[string]*h5Group
	vars            map[string]*h5Var
	attributes      api.AttributeMap
	addr            uint64
	attrVlenIndices map[string][]uint32 // per-attribute vlen heap indices
}

type h5GlobalHeap struct {
	addr    uint64
	objects [][]byte
	indices map[string]uint32
}

type h5Message struct {
	mType uint16
	data  []byte
	flags uint8
}

type HDF5Writer struct {
	file            *os.File
	buf             *bytes.Buffer
	root            *h5Group
	heap            *h5GlobalHeap
	closed          bool
	dimAddrs        map[string]uint64 // dimension name → dim scale variable OH address
	byteOrder       binary.ByteOrder
	vlenHeapIndices []uint32 // per-variable vlen heap indices during data write
	vlenPos         int      // counter into vlenHeapIndices during data write
}

func (hw *HDF5Writer) Close() (err error) {
	defer thrower.RecoverError(&err)
	if hw.closed {
		return nil
	}
	hw.closed = true

	// 0. Add NetCDF4 metadata
	hw.addNCProperties()
	hw.createDimensionScales(hw.root)
	hw.dimAddrs = make(map[string]uint64)

	// 1. Write Superblock V2 (48 bytes)
	hw.writeSuperblockV2()

	// 2. Write dimension scale variable data and OHs first,
	//    so we know their addresses for DIMENSION_LIST references.
	hw.writeDimScaleContents(hw.root)

	// 3. Collect strings, vlen data, and dimension references, then write global heap
	hw.heap = &h5GlobalHeap{indices: make(map[string]uint32)}
	hw.collectStrings(hw.root)
	hw.collectVlenData(hw.root)
	hw.collectAttrVlenData(hw.root)
	hw.collectDimReferences(hw.root)
	if len(hw.heap.objects) > 0 {
		hw.writeGlobalHeap()
	}

	// 4. Write data variables and subgroups
	hw.writeGroupContents(hw.root)

	// 5. Write Root Group OH V2
	rootAddr := uint64(hw.buf.Len())
	hw.writeGroupObjectHeaderV2(hw.root)

	// 6. Finalize Superblock
	eofAddr := uint64(hw.buf.Len())
	data := hw.buf.Bytes()

	binary.LittleEndian.PutUint64(data[28:], eofAddr)
	binary.LittleEndian.PutUint64(data[36:], rootAddr)

	sbChecksum := checksum(data[:44])
	binary.LittleEndian.PutUint32(data[44:], sbChecksum)

	_, err = hw.file.Write(data)
	if err != nil {
		return err
	}
	err = hw.file.Close()
	return err
}

// addNCProperties adds the _NCProperties hidden attribute to the root group.
func (hw *HDF5Writer) addNCProperties() {
	const ncpValue = "version=2,github.com/batchatco/go-native-netcdf=1.0"
	keys := hw.root.attributes.Keys()
	keys = append(keys, ncpKey)
	vals := make(map[string]any)
	for _, k := range hw.root.attributes.Keys() {
		v, _ := hw.root.attributes.Get(k)
		vals[k] = v
	}
	vals[ncpKey] = ncpValue
	hw.root.attributes, _ = util.NewOrderedMap(keys, vals)
}

// createDimensionScales collects all unique dimensions from variables
// and creates dimension scale variables for each one.
func (hw *HDF5Writer) createDimensionScales(g *h5Group) {
	type dimInfo struct {
		name string
		size uint64
	}
	// Collect unique dimensions in order
	seen := make(map[string]bool)
	var dims []dimInfo

	var collectDims func(grp *h5Group)
	collectDims = func(grp *h5Group) {
		var names []string
		for name := range grp.vars {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			v := grp.vars[name]
			if v.isDimScale {
				continue
			}
			shape := hw.getDimensions(v.val)
			for i, dname := range v.dimensions {
				if dname == "" || seen[dname] {
					continue
				}
				seen[dname] = true
				var size uint64
				if i < len(shape) {
					size = shape[i]
				}
				dims = append(dims, dimInfo{dname, size})
			}
		}
		var groupNames []string
		for name := range grp.groups {
			groupNames = append(groupNames, name)
		}
		slices.Sort(groupNames)
		for _, name := range groupNames {
			collectDims(grp.groups[name])
		}
	}
	collectDims(g)

	// Create or promote dimension scale variables.
	// If a variable already exists with the same name as a dimension
	// (a "coordinate variable"), promote it to a dimension scale by
	// adding the required attributes. Otherwise create a synthetic one.
	for i, d := range dims {
		if existing, ok := g.vars[d.name]; ok && !existing.isDimScale {
			// Promote existing coordinate variable to dimension scale
			existing.isDimScale = true
			// Prepend CLASS, NAME, _Netcdf4Dimid to existing attributes
			nameVal := d.name
			keys := []string{"CLASS", "NAME", "_Netcdf4Dimid"}
			vals := map[string]any{
				"CLASS":         "DIMENSION_SCALE",
				"NAME":          nameVal,
				"_Netcdf4Dimid": int32(i),
			}
			if existing.attributes != nil {
				for _, k := range existing.attributes.Keys() {
					v, _ := existing.attributes.Get(k)
					keys = append(keys, k)
					vals[k] = v
				}
			}
			existing.attributes, _ = util.NewOrderedMap(keys, vals)
		} else {
			// Create synthetic dimension scale variable
			nameVal := fmt.Sprintf("This is a netCDF dimension but not a netCDF variable.%10d", d.size)
			dimAttrs, _ := util.NewOrderedMap(
				[]string{"CLASS", "NAME", "_Netcdf4Dimid"},
				map[string]any{
					"CLASS":         "DIMENSION_SCALE",
					"NAME":          nameVal,
					"_Netcdf4Dimid": int32(i),
				},
			)
			g.vars[d.name] = &h5Var{
				name:       d.name,
				val:        make([]int8, d.size),
				dimensions: []string{d.name},
				attributes: dimAttrs,
				isDimScale: true,
			}
		}
	}
}

// writeDimScaleContents writes dimension scale variables' data and OHs.
func (hw *HDF5Writer) writeDimScaleContents(g *h5Group) {
	var dimNames []string
	for name, v := range g.vars {
		if v.isDimScale {
			dimNames = append(dimNames, name)
		}
	}
	slices.Sort(dimNames)
	for _, name := range dimNames {
		v := g.vars[name]
		dataAddr := uint64(hw.buf.Len())
		dataSize := hw.writeDataVar(v)

		v.addr = uint64(hw.buf.Len())
		hw.writeVarObjectHeaderV2(v, dataAddr, dataSize)
		hw.dimAddrs[name] = v.addr
	}

	// Recurse into subgroups
	for _, sub := range g.groups {
		hw.writeDimScaleContents(sub)
	}
}

// collectDimReferences adds global heap entries for dimension references
// needed by DIMENSION_LIST attributes.
func (hw *HDF5Writer) collectDimReferences(g *h5Group) {
	for _, v := range g.vars {
		if v.isDimScale {
			continue
		}
		for _, dname := range v.dimensions {
			if dname == "" {
				continue
			}
			if _, ok := hw.heap.indices["__dimref:"+dname]; ok {
				continue
			}
			addr, ok := hw.dimAddrs[dname]
			if !ok {
				continue
			}
			// Store the 8-byte object reference in the global heap
			ref := make([]byte, 8)
			binary.LittleEndian.PutUint64(ref, addr)
			hw.heap.objects = append(hw.heap.objects, ref)
			hw.heap.indices["__dimref:"+dname] = uint32(len(hw.heap.objects))
		}
	}
	for _, sub := range g.groups {
		hw.collectDimReferences(sub)
	}
}

func (hw *HDF5Writer) writeGroupContents(g *h5Group) {
	// Write children's data and object headers first
	// (Dimension scale variables were already written in writeDimScaleContents)

	// Data variables (skip dimension scales)
	var varNames []string
	for name, v := range g.vars {
		if !v.isDimScale {
			varNames = append(varNames, name)
		}
	}
	slices.Sort(varNames)
	for _, name := range varNames {
		v := g.vars[name]
		dataAddr := uint64(hw.buf.Len())
		dataSize := hw.writeDataVar(v)

		v.addr = uint64(hw.buf.Len())
		hw.writeVarObjectHeaderV2(v, dataAddr, dataSize)
	}

	// Subgroups
	var groupNames []string
	for name := range g.groups {
		groupNames = append(groupNames, name)
	}
	slices.Sort(groupNames)
	for _, name := range groupNames {
		sub := g.groups[name]
		hw.writeGroupContents(sub)
		sub.addr = uint64(hw.buf.Len())
		hw.writeGroupObjectHeaderV2(sub)
	}
}

func (hw *HDF5Writer) writeGroupObjectHeaderV2(g *h5Group) {
	var messages []h5Message

	// Link Info Message (type 2) - required for the HDF5 library to
	// recognize this object header as a new-style group.
	// Use compact storage (links stored directly in OH).
	messages = append(messages, hw.buildLinkInfoMessage())

	// Group Info Message (type 10)
	messages = append(messages, h5Message{mType: 10, data: []byte{0, 0}})

	// Link messages for children
	var varNames []string
	for name := range g.vars {
		varNames = append(varNames, name)
	}
	slices.Sort(varNames)
	for _, name := range varNames {
		messages = append(messages, hw.buildLinkMessage(name, g.vars[name].addr))
	}

	var groupNames []string
	for name := range g.groups {
		groupNames = append(groupNames, name)
	}
	slices.Sort(groupNames)
	for _, name := range groupNames {
		messages = append(messages, hw.buildLinkMessage(name, g.groups[name].addr))
	}

	// Attributes
	if g.attributes != nil {
		for _, k := range g.attributes.Keys() {
			val, _ := g.attributes.Get(k)
			if indices, ok := g.attrVlenIndices[k]; ok {
				hw.vlenHeapIndices = indices
				hw.vlenPos = 0
			} else {
				hw.vlenHeapIndices = nil
			}
			messages = append(messages, hw.buildAttributeMessage(k, val))
		}
	}

	hw.writeObjectHeaderV2(messages)
}

func (hw *HDF5Writer) writeSuperblockV2() {
	util.MustWriteRaw(hw.buf, []byte(magic))
	util.MustWriteByte(hw.buf, 2)
	util.MustWriteByte(hw.buf, 8)
	util.MustWriteByte(hw.buf, 8)
	util.MustWriteByte(hw.buf, 0)

	temp := make([]byte, 32)
	binary.LittleEndian.PutUint64(temp[0:], 0)                  // Base Address
	binary.LittleEndian.PutUint64(temp[8:], 0xffffffffffffffff) // Superblock Extension Address (invalidAddress)
	binary.LittleEndian.PutUint64(temp[16:], 0)                 // End of File Address
	binary.LittleEndian.PutUint64(temp[24:], 0)                 // Root Group Object Header Address
	util.MustWriteRaw(hw.buf, temp)

	util.MustWriteLE(hw.buf, uint32(0)) // Checksum
}

func (hw *HDF5Writer) writeVarObjectHeaderV2(v *h5Var, dataAddr uint64, dataSize uint64) {
	nDims := len(v.dimensions)
	dtMsg := hw.buildDatatypeMessage(v.val, nDims)
	rv := reflect.ValueOf(v.val)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	dsMsg := buildDataspaceMessage(getDimensionsLimited(rv, nDims))
	layoutMsg := hw.buildLayoutMessageV2(dataAddr, dataSize)

	messages := []h5Message{
		{mType: 1, data: dsMsg},
		{mType: 3, data: dtMsg},
		{mType: 8, data: layoutMsg},
	}

	if v.attributes != nil {
		for _, k := range v.attributes.Keys() {
			val, _ := v.attributes.Get(k)
			if indices, ok := v.attrVlenIndices[k]; ok {
				hw.vlenHeapIndices = indices
				hw.vlenPos = 0
			} else {
				hw.vlenHeapIndices = nil
			}
			messages = append(messages, hw.buildAttributeMessage(k, val))
		}
	}

	// Add DIMENSION_LIST attribute for data variables (not dim scales)
	if !v.isDimScale && len(v.dimensions) > 0 && hw.heap != nil {
		dimListMsg := hw.buildDimensionListAttribute(v.dimensions)
		if dimListMsg != nil {
			messages = append(messages, *dimListMsg)
		}
	}

	hw.writeObjectHeaderV2(messages)
}

func (hw *HDF5Writer) buildLinkInfoMessage() h5Message {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 0)                      // version
	util.MustWriteByte(buf, 0)                      // flags (no creation order tracking)
	util.MustWriteLE(buf, uint64(invalidAddress))   // fractal heap address (undefined = compact)
	util.MustWriteLE(buf, uint64(invalidAddress))   // name index v2 B-tree address (undefined = compact)
	return h5Message{mType: 2, data: buf.Bytes()}
}

func (hw *HDF5Writer) buildLinkMessage(name string, addr uint64) h5Message {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 1) // version
	util.MustWriteByte(buf, 0) // flags
	util.MustWriteByte(buf, byte(len(name)))
	util.MustWriteRaw(buf, []byte(name))
	util.MustWriteLE(buf, addr)

	return h5Message{mType: 6, data: buf.Bytes()}
}

func (hw *HDF5Writer) buildLayoutMessageV2(addr uint64, size uint64) []byte {
	buf := new(bytes.Buffer)
	util.MustWriteByte(buf, 3) // version 3
	util.MustWriteByte(buf, 1) // contiguous
	util.MustWriteLE(buf, addr)
	util.MustWriteLE(buf, size)
	return buf.Bytes()
}

func (hw *HDF5Writer) writeObjectHeaderV2(messages []h5Message) {
	ohBuf := new(bytes.Buffer)
	util.MustWriteRaw(ohBuf, []byte("OHDR"))
	util.MustWriteByte(ohBuf, 2)
	util.MustWriteByte(ohBuf, 0x02) // flags: 4-byte size of chunk 0

	msgBuf := new(bytes.Buffer)
	for _, m := range messages {
		util.MustWriteByte(msgBuf, byte(m.mType))
		util.MustWriteLE(msgBuf, uint16(len(m.data)))
		util.MustWriteByte(msgBuf, m.flags)
		util.MustWriteRaw(msgBuf, m.data)
	}

	// Pad the message block to 8 bytes using a proper null message.
	// The HDF5 spec requires that any unused space >= 4 bytes in a
	// chunk be a proper null message (type 0). Space < 4 bytes is a
	// "gap" which is only valid if preceded by a null message.
	remainder := msgBuf.Len() % 8
	if remainder != 0 {
		pad := 8 - remainder
		if pad < 4 {
			// Not enough room for a null message header alone,
			// so extend to 8 more bytes to fit a null message.
			pad += 8
		}
		// Write a null message (type 0) with data size = pad - 4
		util.MustWriteByte(msgBuf, 0)                      // type 0 (nil)
		util.MustWriteLE(msgBuf, uint16(pad-4))            // data size
		util.MustWriteByte(msgBuf, 0)                      // flags
		for range pad - 4 {
			util.MustWriteByte(msgBuf, 0) // null data
		}
	}

	util.MustWriteLE(ohBuf, uint32(msgBuf.Len()))
	util.MustWriteRaw(ohBuf, msgBuf.Bytes())
	ohChecksum := checksum(ohBuf.Bytes())
	util.MustWriteLE(ohBuf, ohChecksum)
	util.MustWriteRaw(hw.buf, ohBuf.Bytes())
}
// writeData writes variable data to the buffer and returns the logical
// data size (before alignment padding).
func (hw *HDF5Writer) writeDataVar(v *h5Var) uint64 {
	rv := reflect.ValueOf(v.val)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	fixedLen := 0
	if !hw.shouldUseVLen(rv) {
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.String {
			findMaxLen(rv, &fixedLen)
			fixedLen++ // include null terminator
		}
	}
	// Set up vlen state for this variable
	hw.vlenHeapIndices = v.vlenHeapIndices
	hw.vlenPos = 0

	nDims := len(v.dimensions)
	start := hw.buf.Len()
	hw.writeDataRecursive(rv, fixedLen, nDims)
	dataSize := uint64(hw.buf.Len() - start)
	for (hw.buf.Len() % 8) != 0 {
		util.MustWriteByte(hw.buf, 0)
	}
	return dataSize
}

func (hw *HDF5Writer) writeDataRecursive(rv reflect.Value, fixedLen int, dimRemaining int) {
	// Handle special types before general slice processing
	switch v := rv.Interface().(type) {
	case compound:
		hw.writeCompoundValue(hw.buf, v)
		return
	case opaque:
		util.MustWriteRaw(hw.buf, []byte(v))
		return
	case enumerated:
		hw.writeDataRecursive(reflect.ValueOf(v.values), fixedLen, dimRemaining)
		return
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// When dimRemaining == 0, this slice is vlen data: write descriptor
		if dimRemaining <= 0 {
			idx := hw.vlenHeapIndices[hw.vlenPos]
			util.MustWriteLE(hw.buf, uint32(rv.Len()))
			util.MustWriteLE(hw.buf, hw.heap.addr)
			util.MustWriteLE(hw.buf, idx)
			hw.vlenPos++
			return
		}
		for i := range rv.Len() {
			hw.writeDataRecursive(rv.Index(i), fixedLen, dimRemaining-1)
		}
		return
	}
	if rv.Kind() == reflect.String {
		if fixedLen > 0 {
			str := rv.String()
			util.MustWriteRaw(hw.buf, []byte(str))
			// Pad to fixedLen
			for i := len(str); i < fixedLen; i++ {
				util.MustWriteByte(hw.buf, 0)
			}
		} else {
			// VLen string
			str := rv.String()
			idx := hw.heap.indices[str]
			util.MustWriteLE(hw.buf, uint32(len(str)))
			util.MustWriteLE(hw.buf, hw.heap.addr)
			util.MustWriteLE(hw.buf, idx)
		}
		return
	}
	switch rv.Kind() {
	case reflect.Int8, reflect.Uint8,
		reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32,
		reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		util.MustWrite(hw.buf, hw.byteOrder, rv.Interface())
	default:
		thrower.Throw(ErrUnsupportedType)
	}
}

func (hw *HDF5Writer) shouldUseVLen(rv reflect.Value) bool {
	t := rv.Type()
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.String {
		return false
	}
	// Heuristic: if it's a slice or array of strings, use vlen.
	// If it's a single string, use fixed-length (for compatibility and simplicity).
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	return rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array
}

func (hw *HDF5Writer) collectStrings(g *h5Group) {
	for _, v := range g.vars {
		hw.collectStringsRecursive(reflect.ValueOf(v.val))
		if v.attributes != nil {
			for _, k := range v.attributes.Keys() {
				val, _ := v.attributes.Get(k)
				hw.collectStringsRecursive(reflect.ValueOf(val))
			}
		}
	}
	if g.attributes != nil {
		for _, k := range g.attributes.Keys() {
			val, _ := g.attributes.Get(k)
			hw.collectStringsRecursive(reflect.ValueOf(val))
		}
	}
	for _, sub := range g.groups {
		hw.collectStrings(sub)
	}
}

func (hw *HDF5Writer) collectStringsRecursive(rv reflect.Value) {
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}

	// Walk into compound values to collect string fields
	if c, ok := rv.Interface().(compound); ok {
		for _, f := range c {
			// Strings in compounds are always vlen → add to heap directly
			if s, ok := f.Val.(string); ok {
				hw.addStringToHeap(s)
			} else {
				hw.collectStringsRecursive(reflect.ValueOf(f.Val))
			}
		}
		return
	}

	// Walk into enumerated to check inner values
	if e, ok := rv.Interface().(enumerated); ok {
		hw.collectStringsRecursive(reflect.ValueOf(e.values))
		return
	}

	// Skip opaque (no strings inside)
	if _, ok := rv.Interface().(opaque); ok {
		return
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// For vlen strings, collect them
		if hw.shouldUseVLen(rv) {
			hw.collectStringsActual(rv)
			return
		}
		// Recurse into slices to find compound/enum elements
		for i := range rv.Len() {
			hw.collectStringsRecursive(rv.Index(i))
		}
		return
	}

	// Single string: collect if it's a standalone vlen string
	// (scalar strings in attributes are fixed-length, not collected)
}

func (hw *HDF5Writer) collectStringsActual(rv reflect.Value) {
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := range rv.Len() {
			hw.collectStringsActual(rv.Index(i))
		}
		return
	}
	if rv.Kind() == reflect.String {
		hw.addStringToHeap(rv.String())
	}
}

func (hw *HDF5Writer) addStringToHeap(s string) {
	if _, ok := hw.heap.indices[s]; !ok {
		hw.heap.objects = append(hw.heap.objects, []byte(s))
		hw.heap.indices[s] = uint32(len(hw.heap.objects))
	}
}

// collectVlenData walks all variables and serializes vlen elements into the global heap.
func (hw *HDF5Writer) collectVlenData(g *h5Group) {
	for _, v := range g.vars {
		nDims := len(v.dimensions)
		rv := reflect.ValueOf(v.val)
		for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
			rv = rv.Elem()
		}
		// Check if this variable has vlen data (more nesting than declared dims)
		if hw.hasVlenData(rv, nDims) {
			hw.collectVlenElements(rv, nDims, v)
		}
	}
	for _, sub := range g.groups {
		hw.collectVlenData(sub)
	}
}

// hasVlenData checks if a value has more nesting levels than nDims,
// meaning it contains vlen data.
func (hw *HDF5Writer) hasVlenData(rv reflect.Value, nDims int) bool {
	if e, ok := rv.Interface().(enumerated); ok {
		return hw.hasVlenData(reflect.ValueOf(e.values), nDims)
	}
	for range nDims {
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return false
		}
		if rv.Type() == reflect.TypeOf(compound{}) || rv.Type() == reflect.TypeOf(opaque{}) {
			return false
		}
		if rv.Len() == 0 {
			// Check type: if element type is still a slice, it's vlen
			elemType := rv.Type().Elem()
			return elemType.Kind() == reflect.Slice &&
				elemType != reflect.TypeOf(compound{}) &&
				elemType != reflect.TypeOf(opaque{})
		}
		rv = rv.Index(0)
	}
	// After consuming nDims, check if there's still a slice (= vlen)
	if rv.Kind() == reflect.Slice && rv.Type() != reflect.TypeOf(compound{}) && rv.Type() != reflect.TypeOf(opaque{}) {
		return true
	}
	// Also check within compound fields
	if c, ok := rv.Interface().(compound); ok {
		for _, f := range c {
			frv := reflect.ValueOf(f.Val)
			if frv.Kind() == reflect.Slice && frv.Type() != reflect.TypeOf(compound{}) && frv.Type() != reflect.TypeOf(opaque{}) {
				return true
			}
		}
	}
	return false
}

// collectVlenElements walks through the array dimensions and serializes
// each vlen element into the global heap.
func (hw *HDF5Writer) collectVlenElements(rv reflect.Value, dimsLeft int, v *h5Var) {
	if e, ok := rv.Interface().(enumerated); ok {
		hw.collectVlenElements(reflect.ValueOf(e.values), dimsLeft, v)
		return
	}
	if dimsLeft > 0 && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		for i := range rv.Len() {
			hw.collectVlenElements(rv.Index(i), dimsLeft-1, v)
		}
		return
	}
	// At the vlen level: serialize this element's data
	if rv.Kind() == reflect.Slice {
		hw.serializeVlenElement(rv, v)
		return
	}
	// Check compound fields for vlen
	if c, ok := rv.Interface().(compound); ok {
		for _, f := range c {
			frv := reflect.ValueOf(f.Val)
			if frv.Kind() == reflect.Slice && frv.Type() != reflect.TypeOf(compound{}) && frv.Type() != reflect.TypeOf(opaque{}) {
				hw.serializeVlenElement(frv, v)
			}
		}
	}
}

// collectAttrVlenData walks all attributes (on variables and groups) and
// serializes vlen elements into the global heap.
func (hw *HDF5Writer) collectAttrVlenData(g *h5Group) {
	for _, v := range g.vars {
		if v.attributes != nil {
			for _, k := range v.attributes.Keys() {
				val, _ := v.attributes.Get(k)
				var indices []uint32
				hw.collectVlenFromValue(reflect.ValueOf(val), &indices)
				if len(indices) > 0 {
					if v.attrVlenIndices == nil {
						v.attrVlenIndices = make(map[string][]uint32)
					}
					v.attrVlenIndices[k] = indices
				}
			}
		}
	}
	if g.attributes != nil {
		for _, k := range g.attributes.Keys() {
			val, _ := g.attributes.Get(k)
			var indices []uint32
			hw.collectVlenFromValue(reflect.ValueOf(val), &indices)
			if len(indices) > 0 {
				if g.attrVlenIndices == nil {
					g.attrVlenIndices = make(map[string][]uint32)
				}
				g.attrVlenIndices[k] = indices
			}
		}
	}
	for _, sub := range g.groups {
		hw.collectAttrVlenData(sub)
	}
}

// collectVlenFromValue recursively walks a value and serializes any vlen
// elements (varying-length slices) to the global heap.
func (hw *HDF5Writer) collectVlenFromValue(rv reflect.Value, indices *[]uint32) {
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}

	switch v := rv.Interface().(type) {
	case compound:
		for _, f := range v {
			frv := reflect.ValueOf(f.Val)
			if frv.Kind() == reflect.Slice &&
				frv.Type() != reflect.TypeOf(compound{}) &&
				frv.Type() != reflect.TypeOf(opaque{}) {
				// Vlen field in compound: serialize it
				hw.serializeVlenToHeap(frv, indices)
			} else {
				hw.collectVlenFromValue(frv, indices)
			}
		}
		return
	case opaque:
		return
	case enumerated:
		hw.collectVlenFromValue(reflect.ValueOf(v.values), indices)
		return
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Type() == reflect.TypeOf(compound{}) || rv.Type() == reflect.TypeOf(opaque{}) {
			return
		}
		// Check if elements are slices with varying lengths (vlen)
		if rv.Len() > 0 && rv.Index(0).Kind() == reflect.Slice {
			isVlen := false
			if rv.Len() > 1 {
				firstLen := rv.Index(0).Len()
				for i := 1; i < rv.Len(); i++ {
					if rv.Index(i).Len() != firstLen {
						isVlen = true
						break
					}
				}
			}
			if isVlen {
				for i := range rv.Len() {
					hw.serializeVlenToHeap(rv.Index(i), indices)
				}
				return
			}
		}
		// Regular array: recurse into elements
		for i := range rv.Len() {
			hw.collectVlenFromValue(rv.Index(i), indices)
		}
	}
}

// serializeVlenToHeap serializes a single vlen slice and adds it to the heap.
func (hw *HDF5Writer) serializeVlenToHeap(rv reflect.Value, indices *[]uint32) {
	buf := new(bytes.Buffer)
	for i := range rv.Len() {
		elem := rv.Index(i)
		switch val := elem.Interface().(type) {
		case compound:
			hw.writeCompoundValue(buf, val)
		case opaque:
			util.MustWriteRaw(buf, []byte(val))
		case enumerated:
			util.MustWrite(buf, hw.byteOrder, val.values)
		default:
			util.MustWrite(buf, hw.byteOrder, val)
		}
	}
	data := buf.Bytes()
	hw.heap.objects = append(hw.heap.objects, data)
	*indices = append(*indices, uint32(len(hw.heap.objects)))
}

// getAttributeDimensions returns the array dimensions and effective nDims
// for an attribute value, detecting vlen (varying inner lengths) and stopping.
func (hw *HDF5Writer) getAttributeDimensions(val any) ([]uint64, int) {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	if e, ok := rv.Interface().(enumerated); ok {
		return hw.getAttributeDimensions(e.values)
	}
	var dims []uint64
	nDims := 0
	for rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Type() == reflect.TypeOf(compound{}) || rv.Type() == reflect.TypeOf(opaque{}) {
			break
		}
		dims = append(dims, uint64(rv.Len()))
		nDims++
		if rv.Len() == 0 {
			break
		}
		// Check if inner elements have varying lengths (= vlen boundary)
		if rv.Len() > 1 && rv.Index(0).Kind() == reflect.Slice {
			firstLen := rv.Index(0).Len()
			isVlen := false
			for i := 1; i < rv.Len(); i++ {
				if rv.Index(i).Len() != firstLen {
					isVlen = true
					break
				}
			}
			if isVlen {
				break
			}
		}
		rv = rv.Index(0)
	}
	return dims, nDims
}

// serializeVlenElement serializes a single vlen element and adds it to the heap.
func (hw *HDF5Writer) serializeVlenElement(rv reflect.Value, v *h5Var) {
	buf := new(bytes.Buffer)
	for i := range rv.Len() {
		elem := rv.Index(i)
		switch val := elem.Interface().(type) {
		case compound:
			hw.writeCompoundValue(buf, val)
		case opaque:
			util.MustWriteRaw(buf, []byte(val))
		case enumerated:
			util.MustWrite(buf, hw.byteOrder, val.values)
		default:
			util.MustWrite(buf, hw.byteOrder, val)
		}
	}
	data := buf.Bytes()
	hw.heap.objects = append(hw.heap.objects, data)
	v.vlenHeapIndices = append(v.vlenHeapIndices, uint32(len(hw.heap.objects)))
}

func (hw *HDF5Writer) writeGlobalHeap() {
	gh := hw.heap

	// Align to 8 bytes so that objects within the collection are
	// properly aligned when using absolute buffer position for padding.
	for (hw.buf.Len() % 8) != 0 {
		util.MustWriteByte(hw.buf, 0)
	}
	gh.addr = uint64(hw.buf.Len())

	// Collection Header
	util.MustWriteRaw(hw.buf, []byte("GCOL"))
	util.MustWriteByte(hw.buf, 1)              // version
	util.MustWriteRaw(hw.buf, []byte{0, 0, 0}) // reserved

	sizeAddr := hw.buf.Len()
	util.MustWriteLE(hw.buf, uint64(0)) // placeholder for size

	for i, obj := range gh.objects {
		util.MustWriteLE(hw.buf, uint16(i+1)) // index
		util.MustWriteLE(hw.buf, uint16(0))   // ref count
		util.MustWriteLE(hw.buf, uint32(0))   // reserved
		util.MustWriteLE(hw.buf, uint64(len(obj)))
		util.MustWriteRaw(hw.buf, obj)
		// Pad object to 8 bytes
		for (hw.buf.Len() % 8) != 0 {
			util.MustWriteByte(hw.buf, 0)
		}
	}

	// The HDF5 library requires global heap collections to be at least
	// 4096 bytes (H5HG_MINSIZE). Pad with free space to meet this minimum.
	// The free space entry (index 0) size field includes its own 16-byte
	// header — it represents the total remaining bytes from the start of
	// the entry to the end of the collection.
	const minGCOLSize = 4096
	currentSize := hw.buf.Len() - int(gh.addr)
	totalSize := currentSize + 16 // at least room for the free space header
	if totalSize < minGCOLSize {
		totalSize = minGCOLSize
	}
	freeEntrySize := totalSize - currentSize // includes the 16-byte header
	util.MustWriteLE(hw.buf, uint16(0))               // index 0 = free space
	util.MustWriteLE(hw.buf, uint16(0))               // ref count
	util.MustWriteLE(hw.buf, uint32(0))               // reserved
	util.MustWriteLE(hw.buf, uint64(freeEntrySize))   // total free space (incl header)
	for range freeEntrySize - 16 {
		util.MustWriteByte(hw.buf, 0)
	}

	// Final size
	finalSize := uint64(hw.buf.Len()) - gh.addr
	data := hw.buf.Bytes()
	binary.LittleEndian.PutUint64(data[sizeAddr:], finalSize)
}

func (hw *HDF5Writer) getDimensions(val any) []uint64 {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	return getDimensionsRecursive(rv)
}

func getDimensionsRecursive(rv reflect.Value) []uint64 {
	// Unwrap enumerated to its inner values
	if e, ok := rv.Interface().(enumerated); ok {
		return getDimensionsRecursive(reflect.ValueOf(e.values))
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// Stop at compound and opaque (named slice types that are leaf values)
		if rv.Type() == reflect.TypeOf(compound{}) || rv.Type() == reflect.TypeOf(opaque{}) {
			return nil
		}
		dims := []uint64{uint64(rv.Len())}
		if rv.Len() > 0 {
			inner := getDimensionsRecursive(rv.Index(0))
			dims = append(dims, inner...)
		}
		return dims
	}
	return nil
}

// getDimensionsLimited returns dimensions up to maxDepth levels deep.
// For vlen variables, the array nesting exceeds the declared dimensions;
// this function stops after maxDepth levels so the excess nesting can be
// identified as vlen data.
func getDimensionsLimited(rv reflect.Value, maxDepth int) []uint64 {
	if e, ok := rv.Interface().(enumerated); ok {
		return getDimensionsLimited(reflect.ValueOf(e.values), maxDepth)
	}
	if maxDepth <= 0 {
		return nil
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Type() == reflect.TypeOf(compound{}) || rv.Type() == reflect.TypeOf(opaque{}) {
			return nil
		}
		dims := []uint64{uint64(rv.Len())}
		if rv.Len() > 0 {
			inner := getDimensionsLimited(rv.Index(0), maxDepth-1)
			dims = append(dims, inner...)
		}
		return dims
	}
	return nil
}

func (hw *HDF5Writer) AddAttributes(attrs api.AttributeMap) error {
	if !hasValidNames(attrs) {
		return ErrInvalidName
	}
	if attrs != nil {
		for _, k := range attrs.Keys() {
			v, _ := attrs.Get(k)
			if !isSupportedType(v) {
				return fmt.Errorf("%w: %T", ErrUnsupportedType, v)
			}
		}
	}
	hw.root.attributes = attrs
	return nil
}

func (hw *HDF5Writer) AddVar(name string, vr api.Variable) error {
	if !internal.IsValidNetCDFName(name) {
		return ErrInvalidName
	}
	if !hasValidNames(vr.Attributes) {
		return ErrInvalidName
	}
	if !isSupportedType(vr.Values) {
		return fmt.Errorf("%w: %T", ErrUnsupportedType, vr.Values)
	}
	if vr.Attributes != nil {
		for _, k := range vr.Attributes.Keys() {
			v, _ := vr.Attributes.Get(k)
			if !isSupportedType(v) {
				return fmt.Errorf("%w: %T", ErrUnsupportedType, v)
			}
		}
	}
	hw.root.vars[name] = &h5Var{
		name:       name,
		val:        vr.Values,
		dimensions: vr.Dimensions,
		attributes: vr.Attributes,
	}
	return nil
}

func (hw *HDF5Writer) CreateGroup(name string) (api.Writer, error) {
	if !internal.IsValidNetCDFName(name) {
		return nil, ErrInvalidName
	}
	if g, ok := hw.root.groups[name]; ok {
		return &groupWriter{hw: hw, group: g}, nil
	}
	g := &h5Group{
		name:   name,
		groups: make(map[string]*h5Group),
		vars:   make(map[string]*h5Var),
	}
	hw.root.groups[name] = g
	return &groupWriter{hw: hw, group: g}, nil
}

type groupWriter struct {
	hw    *HDF5Writer
	group *h5Group
}

func (gw *groupWriter) Close() error {
	return nil
}

func (gw *groupWriter) AddAttributes(attrs api.AttributeMap) error {
	if !hasValidNames(attrs) {
		return ErrInvalidName
	}
	if attrs != nil {
		for _, k := range attrs.Keys() {
			v, _ := attrs.Get(k)
			if !isSupportedType(v) {
				return fmt.Errorf("%w: %T", ErrUnsupportedType, v)
			}
		}
	}
	gw.group.attributes = attrs
	return nil
}

func (gw *groupWriter) AddVar(name string, vr api.Variable) error {
	if !internal.IsValidNetCDFName(name) {
		return ErrInvalidName
	}
	if !hasValidNames(vr.Attributes) {
		return ErrInvalidName
	}
	if !isSupportedType(vr.Values) {
		return fmt.Errorf("%w: %T", ErrUnsupportedType, vr.Values)
	}
	if vr.Attributes != nil {
		for _, k := range vr.Attributes.Keys() {
			v, _ := vr.Attributes.Get(k)
			if !isSupportedType(v) {
				return fmt.Errorf("%w: %T", ErrUnsupportedType, v)
			}
		}
	}
	gw.group.vars[name] = &h5Var{
		name:       name,
		val:        vr.Values,
		dimensions: vr.Dimensions,
		attributes: vr.Attributes,
	}
	return nil
}

func (gw *groupWriter) CreateGroup(name string) (api.Writer, error) {
	if !internal.IsValidNetCDFName(name) {
		return nil, ErrInvalidName
	}
	if g, ok := gw.group.groups[name]; ok {
		return &groupWriter{hw: gw.hw, group: g}, nil
	}
	g := &h5Group{
		name:   name,
		groups: make(map[string]*h5Group),
		vars:   make(map[string]*h5Var),
	}
	gw.group.groups[name] = g
	return &groupWriter{hw: gw.hw, group: g}, nil
}

// isSupportedType checks whether val is a type the HDF5/NetCDF4 writer can
// handle. It recursively unwraps slices/arrays (stopping at compound/opaque)
// and checks that the leaf type is one of the supported scalar types.
func isSupportedType(val any) bool {
	switch v := val.(type) {
	case int8, uint8, int16, uint16, int32, uint32, int64, uint64,
		float32, float64, string:
		return true
	case compound:
		for _, f := range v {
			if !isSupportedType(f.Val) {
				return false
			}
		}
		return true
	case opaque:
		return true
	case enumerated:
		return isSupportedType(v.values)
	}

	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		t := rv.Type()
		if t == reflect.TypeOf(compound{}) || t == reflect.TypeOf(opaque{}) {
			return true
		}
		if rv.Len() == 0 {
			// Check element type of empty slice
			return isSupportedElemType(t.Elem())
		}
		rv = rv.Index(0)
		// Check for named types at element level
		switch rv.Interface().(type) {
		case compound, opaque, enumerated:
			return isSupportedType(rv.Interface())
		}
	}

	// Check the leaf scalar
	switch rv.Kind() {
	case reflect.Int8, reflect.Uint8,
		reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32,
		reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	}
	return false
}

// isSupportedElemType checks whether a reflect.Type (which may be a slice
// element type from an empty slice) eventually resolves to a supported leaf.
func isSupportedElemType(t reflect.Type) bool {
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t == reflect.TypeOf(compound{}) || t == reflect.TypeOf(opaque{}) {
			return true
		}
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Int8, reflect.Uint8,
		reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32,
		reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	}
	// Also accept enumerated struct type
	if t == reflect.TypeOf(enumerated{}) {
		return true
	}
	return false
}

func hasValidNames(am api.AttributeMap) bool {
	if am == nil {
		return true
	}
	for _, key := range am.Keys() {
		if !internal.IsValidNetCDFName(key) {
			return false
		}
	}
	return true
}

func OpenWriter(fileName string) (api.Writer, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	rootAttrs, _ := util.NewOrderedMap(nil, nil)
	hw := &HDF5Writer{
		file:      file,
		buf:       new(bytes.Buffer),
		byteOrder: util.NativeByteOrder,
		root: &h5Group{
			name:       "/",
			groups:     make(map[string]*h5Group),
			vars:       make(map[string]*h5Var),
			attributes: rootAttrs,
		},
	}
	return hw, nil
}
