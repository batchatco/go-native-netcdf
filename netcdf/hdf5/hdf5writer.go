package hdf5

import (
	"bytes"
	"encoding/binary"
	"os"
	"reflect"
	"slices"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
)

type h5Var struct {
	name       string
	val        any
	dimensions []string
	attributes api.AttributeMap
	addr       uint64
}

type h5Group struct {
	name       string
	groups     map[string]*h5Group
	vars       map[string]*h5Var
	attributes api.AttributeMap
	addr       uint64
}

type h5GlobalHeap struct {
	addr    uint64
	objects [][]byte
	indices map[string]uint32
}

type HDF5Writer struct {
	file   *os.File
	buf    *bytes.Buffer
	root   *h5Group
	heap   *h5GlobalHeap
	closed bool
}

func (hw *HDF5Writer) Close() (err error) {
	defer thrower.RecoverError(&err)
	if hw.closed {
		return nil
	}
	hw.closed = true

	// 1. Write Superblock V2 (48 bytes)
	hw.writeSuperblockV2()

	// 1.5 Collect and write global heap if needed
	hw.heap = &h5GlobalHeap{indices: make(map[string]uint32)}
	hw.collectStrings(hw.root)
	if len(hw.heap.objects) > 0 {
		hw.writeGlobalHeap()
	}

	// 2. Write variables and subgroups recursively
	hw.writeGroupContents(hw.root)

	// 3. Write Root Group OH V2
	rootAddr := uint64(hw.buf.Len())
	hw.writeGroupObjectHeaderV2(hw.root)

	// 4. Finalize Superblock
	eofAddr := uint64(hw.buf.Len())
	data := hw.buf.Bytes()

	// Superblock V2 offsets:
	// Magic (8), Version (1), Offsets Size (1), Lengths Size (1), Flags (1) = 12 bytes
	// Base Address (8): 12-19
	// SB Extension (8): 20-27
	// EOF Address (8): 28-35
	// Root OH Address (8): 36-43
	// Checksum (4): 44-47
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

func (hw *HDF5Writer) writeGroupContents(g *h5Group) {
	// Write children's data and object headers first

	// Variables
	var varNames []string
	for name := range g.vars {
		varNames = append(varNames, name)
	}
	slices.Sort(varNames)
	for _, name := range varNames {
		v := g.vars[name]
		dataAddr := uint64(hw.buf.Len())
		hw.writeData(v.val)
		dataSize := uint64(hw.buf.Len()) - dataAddr

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
	dtMsg := hw.buildDatatypeMessage(v.val)
	dsMsg := buildDataspaceMessage(hw.getDimensions(v.val))
	layoutMsg := hw.buildLayoutMessageV2(dataAddr, dataSize)

	messages := []h5Message{
		{mType: 1, data: dsMsg},
		{mType: 3, data: dtMsg},
		{mType: 8, data: layoutMsg},
	}

	if v.attributes != nil {
		for _, k := range v.attributes.Keys() {
			val, _ := v.attributes.Get(k)
			messages = append(messages, hw.buildAttributeMessage(k, val))
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
func (hw *HDF5Writer) writeData(val any) {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	fixedLen := 0
	if !hw.shouldUseVLen(rv) {
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.String {
			findMaxLen(rv, &fixedLen)
			fixedLen++ // include null terminator
		}
	}
	hw.writeDataRecursive(rv, fixedLen)
	for (hw.buf.Len() % 8) != 0 {
		util.MustWriteByte(hw.buf, 0)
	}
}

func (hw *HDF5Writer) writeDataRecursive(rv reflect.Value, fixedLen int) {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := range rv.Len() {
			hw.writeDataRecursive(rv.Index(i), fixedLen)
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
	util.MustWriteLE(hw.buf, rv.Interface())
}

func (hw *HDF5Writer) shouldUseVLen(rv reflect.Value) bool {
	t := rv.Type()
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.String {
		return false
	}
	// Heuristic: if it's a slice or array of strings, use vlen.
	// If it's a single string, use fixed-length (for compatibility and simplicity).
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
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
	if !hw.shouldUseVLen(rv) {
		return
	}
	hw.collectStringsRecursiveActual(rv)
}

func (hw *HDF5Writer) collectStringsRecursiveActual(rv reflect.Value) {
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := range rv.Len() {
			hw.collectStringsRecursiveActual(rv.Index(i))
		}
		return
	}
	if rv.Kind() == reflect.String {
		s := rv.String()
		if _, ok := hw.heap.indices[s]; !ok {
			hw.heap.objects = append(hw.heap.objects, []byte(s))
			hw.heap.indices[s] = uint32(len(hw.heap.objects))
		}
	}
}

func (hw *HDF5Writer) writeGlobalHeap() {
	gh := hw.heap
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

	// Final size
	totalSize := uint64(hw.buf.Len()) - gh.addr
	data := hw.buf.Bytes()
	binary.LittleEndian.PutUint64(data[sizeAddr:], totalSize)
}

func (hw *HDF5Writer) getDimensions(val any) []uint64 {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	return getDimensionsRecursive(rv)
}

func getDimensionsRecursive(rv reflect.Value) []uint64 {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		dims := []uint64{uint64(rv.Len())}
		if rv.Len() > 0 {
			inner := getDimensionsRecursive(rv.Index(0))
			dims = append(dims, inner...)
		}
		return dims
	}
	return nil
}

func (hw *HDF5Writer) AddAttributes(attrs api.AttributeMap) error {
	hw.root.attributes = attrs
	return nil
}

func (hw *HDF5Writer) AddVar(name string, vr api.Variable) error {
	hw.root.vars[name] = &h5Var{
		name:       name,
		val:        vr.Values,
		dimensions: vr.Dimensions,
		attributes: vr.Attributes,
	}
	return nil
}

func (hw *HDF5Writer) CreateGroup(name string) (api.Writer, error) {
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
	gw.group.attributes = attrs
	return nil
}

func (gw *groupWriter) AddVar(name string, vr api.Variable) error {
	gw.group.vars[name] = &h5Var{
		name:       name,
		val:        vr.Values,
		dimensions: vr.Dimensions,
		attributes: vr.Attributes,
	}
	return nil
}

func (gw *groupWriter) CreateGroup(name string) (api.Writer, error) {
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

func OpenWriter(fileName string) (api.Writer, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	rootAttrs, _ := util.NewOrderedMap(nil, nil)
	hw := &HDF5Writer{
		file: file,
		buf:  new(bytes.Buffer),
		root: &h5Group{
			name:       "/",
			groups:     make(map[string]*h5Group),
			vars:       make(map[string]*h5Var),
			attributes: rootAttrs,
		},
	}
	return hw, nil
}

type h5Message struct {
	mType uint16
	data  []byte
	flags uint8
}
