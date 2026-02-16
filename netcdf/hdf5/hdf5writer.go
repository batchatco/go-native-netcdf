package hdf5

import (
	"bytes"
	"encoding/binary"
	"os"
	"reflect"
	"sort"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
)

type h5Var struct {
	name       string
	val        interface{}
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

type HDF5Writer struct {
	file   *os.File
	buf    *bytes.Buffer
	root   *h5Group
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
	sort.Strings(varNames)
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
	sort.Strings(groupNames)
	for _, name := range groupNames {
		sub := g.groups[name]
		hw.writeGroupContents(sub)
		sub.addr = uint64(hw.buf.Len())
		hw.writeGroupObjectHeaderV2(sub)
	}
}

func (hw *HDF5Writer) writeGroupObjectHeaderV2(g *h5Group) {
	var messages []h5Message
	
	// Group Info Message (type 10)
	messages = append(messages, h5Message{mType: 10, data: []byte{0, 0}})
	
	// Link messages for children
	var varNames []string
	for name := range g.vars {
		varNames = append(varNames, name)
	}
	sort.Strings(varNames)
	for _, name := range varNames {
		messages = append(messages, hw.buildLinkMessage(name, g.vars[name].addr))
	}
	
	var groupNames []string
	for name := range g.groups {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)
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
	_, err := hw.buf.Write([]byte(magic))
	thrower.ThrowIfError(err)
	err = hw.buf.WriteByte(2)
	thrower.ThrowIfError(err)
	err = hw.buf.WriteByte(8)
	thrower.ThrowIfError(err)
	err = hw.buf.WriteByte(8)
	thrower.ThrowIfError(err)
	err = hw.buf.WriteByte(0)
	thrower.ThrowIfError(err)
	
	temp := make([]byte, 32)
	binary.LittleEndian.PutUint64(temp[0:], 0)              // Base Address
	binary.LittleEndian.PutUint64(temp[8:], 0xffffffffffffffff) // Superblock Extension Address (invalidAddress)
	binary.LittleEndian.PutUint64(temp[16:], 0)             // End of File Address
	binary.LittleEndian.PutUint64(temp[24:], 0)             // Root Group Object Header Address
	_, err = hw.buf.Write(temp)
	thrower.ThrowIfError(err)
	
	err = binary.Write(hw.buf, binary.LittleEndian, uint32(0)) // Checksum
	thrower.ThrowIfError(err)
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

func (hw *HDF5Writer) buildLinkMessage(name string, addr uint64) h5Message {
	buf := new(bytes.Buffer)
	buf.WriteByte(1) // version
	buf.WriteByte(0) // flags
	buf.WriteByte(byte(len(name)))
	buf.Write([]byte(name))
	binary.Write(buf, binary.LittleEndian, addr)
	
	return h5Message{mType: 6, data: buf.Bytes()}
}

func (hw *HDF5Writer) buildLayoutMessageV2(addr uint64, size uint64) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(3) // version 3
	buf.WriteByte(1) // contiguous
	binary.Write(buf, binary.LittleEndian, addr)
	binary.Write(buf, binary.LittleEndian, size)
	return buf.Bytes()
}

func (hw *HDF5Writer) writeObjectHeaderV2(messages []h5Message) {
	ohBuf := new(bytes.Buffer)
	_, err := ohBuf.Write([]byte("OHDR"))
	thrower.ThrowIfError(err)
	err = ohBuf.WriteByte(2)
	thrower.ThrowIfError(err)
	err = ohBuf.WriteByte(0x02) // flags: 4-byte size of chunk 0
	thrower.ThrowIfError(err)
	
	msgBuf := new(bytes.Buffer)
	for _, m := range messages {
		err = msgBuf.WriteByte(byte(m.mType))
		thrower.ThrowIfError(err)
		err = binary.Write(msgBuf, binary.LittleEndian, uint16(len(m.data)))
		thrower.ThrowIfError(err)
		err = msgBuf.WriteByte(m.flags)
		thrower.ThrowIfError(err)
		_, err = msgBuf.Write(m.data)
		thrower.ThrowIfError(err)
	}
	
	// Pad the entire message block to 8 bytes
	for (msgBuf.Len() % 8) != 0 {
		err = msgBuf.WriteByte(0) // Type 0 (NIL message) would be better, but padding is just zeros
		thrower.ThrowIfError(err)
	}
	
	err = binary.Write(ohBuf, binary.LittleEndian, uint32(msgBuf.Len()))
	thrower.ThrowIfError(err)
	_, err = ohBuf.Write(msgBuf.Bytes())
	thrower.ThrowIfError(err)
	
	ohChecksum := checksum(ohBuf.Bytes())
	err = binary.Write(ohBuf, binary.LittleEndian, ohChecksum)
	thrower.ThrowIfError(err)
	
	_, err = hw.buf.Write(ohBuf.Bytes())
	thrower.ThrowIfError(err)
}

func (hw *HDF5Writer) writeData(val interface{}) {
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	fixedLen := 0
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.String {
		findMaxLen(rv, &fixedLen)
		fixedLen++ // include null terminator
	}
	hw.writeDataRecursive(rv, fixedLen)
	for (hw.buf.Len() % 8) != 0 {
		hw.buf.WriteByte(0)
	}
}

func (hw *HDF5Writer) writeDataRecursive(rv reflect.Value, fixedLen int) {
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := 0; i < rv.Len(); i++ {
			hw.writeDataRecursive(rv.Index(i), fixedLen)
		}
		return
	}
	if rv.Kind() == reflect.String {
		str := rv.String()
		_, err := hw.buf.Write([]byte(str))
		thrower.ThrowIfError(err)
		// Pad to fixedLen
		for i := len(str); i < fixedLen; i++ {
			err = hw.buf.WriteByte(0)
			thrower.ThrowIfError(err)
		}
		return
	}
	err := binary.Write(hw.buf, binary.LittleEndian, rv.Interface())
	thrower.ThrowIfError(err)
}

func (hw *HDF5Writer) getDimensions(val interface{}) []uint64 {
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

func (hw *HDF5Writer) AddAttributes(attrs api.AttributeMap) (err error) {
	defer thrower.RecoverError(&err)
	hw.root.attributes = attrs
	return nil
}

func (hw *HDF5Writer) AddVar(name string, vr api.Variable) (err error) {
	defer thrower.RecoverError(&err)
	hw.root.vars[name] = &h5Var{
		name:       name,
		val:        vr.Values,
		dimensions: vr.Dimensions,
		attributes: vr.Attributes,
	}
	return nil
}

func (hw *HDF5Writer) CreateGroup(name string) (w api.Writer, err error) {
	defer thrower.RecoverError(&err)
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

func (gw *groupWriter) AddAttributes(attrs api.AttributeMap) (err error) {
	defer thrower.RecoverError(&err)
	gw.group.attributes = attrs
	return nil
}

func (gw *groupWriter) AddVar(name string, vr api.Variable) (err error) {
	defer thrower.RecoverError(&err)
	gw.group.vars[name] = &h5Var{
		name:       name,
		val:        vr.Values,
		dimensions: vr.Dimensions,
		attributes: vr.Attributes,
	}
	return nil
}

func (gw *groupWriter) CreateGroup(name string) (w api.Writer, err error) {
	defer thrower.RecoverError(&err)
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
			name:   "/",
			groups: make(map[string]*h5Group),
			vars:   make(map[string]*h5Var),
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
