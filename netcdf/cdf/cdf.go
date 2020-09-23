// Package cdf supports v1 (classic), v2 (64-bit offset) and v5 file formats.
package cdf

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"

	"github.com/batchatco/go-native-netcdf/internal"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
)

const (
	fieldDimension = 0x0000000a
	fieldVariable  = 0x0000000b
	fieldAttribute = 0x0000000c
)

const (
	typeNone = iota // Never stored in a file: only a sentinal value
	typeByte        // same as go int8
	typeChar        // same as go string when in an array
	typeShort
	typeInt
	typeFloat
	typeDouble

	// v5
	typeUByte // same as go uint8
	typeUShort
	typeUInt
	typeInt64
	typeUInt64
)

const ncpKey = "_NCProperties"

type dimension struct {
	name      string
	dimLength uint64 // 64-bits in V5
}

type variable struct {
	name   string
	dimids []uint64 // 64-bits in V5
	attrs  *util.OrderedMap
	vType  uint32
	vsize  int64  // 64-bits in V5
	begin  uint64 // 32-bits in V1, 64-bits in V2
}

type convertType bool

// constants for convertType
const (
	fast = false // fast conversion, avoids some reflection.
	slow = true  // slow conversion, uses reflection for everything.
)

type CDF struct {
	fname        string
	file         api.ReadSeekerCloser
	fileRefCount int
	version      uint8
	numRecs      uint64 // 64-bits in V5
	recSize      uint64
	dimensions   []dimension
	globalAttrs  *util.OrderedMap
	vars         *util.OrderedMap
	specialCase  bool
	slowConvert  convertType // default is fast
}

const maxDimensions = 1024

var (
	ErrNotCDF                = errors.New("not a CDF file")
	ErrUnsupportedVersion    = errors.New("unsupported CDF version")
	ErrUnknownVersion        = errors.New("unknown CDF version")
	ErrUnknownType           = errors.New("unknown type")
	ErrCorruptedFile         = errors.New("corrupted file")
	ErrNotFound              = errors.New("not found")
	ErrNoStreamingDimensions = errors.New("streaming dimensions not supported")
	ErrInternal              = errors.New("internal error")
	ErrDuplicateVariable     = errors.New("duplicate variable")
	ErrTooManyDimensions     = errors.New("too many dimensions")
	ErrFillValue             = errors.New("fill value not a scalar")
)

var (
	logger = internal.NewLogger()
)

// ListVariables lists the variables in this group.
func (cdf *CDF) ListVariables() []string {
	return cdf.vars.Keys()
}

// ListSubgroups returns the names of the subgroups of this group
func (cdf *CDF) ListSubgroups() []string {
	return nil
}

// ListDimensions lists the names of the dimensions.
func (cdf *CDF) ListDimensions() []string {
	var ret []string
	for _, d := range cdf.dimensions {
		ret = append(ret, d.name)
	}
	return ret
}

// GetDimension returns the value of the named dimension.
func (cdf *CDF) GetDimension(name string) (uint64, bool) {
	for _, d := range cdf.dimensions {
		if d.name == name {
			return d.dimLength, true
		}
	}
	return 0, false
}

// ListTypes just returns an empty list because there are no user-defined types in CDF
func (cdf *CDF) ListTypes() []string {
	return []string{}
}

// GetType always returns false.
// There are no user-defined types in CDF
func (cdf *CDF) GetType(name string) (string, bool) {
	return "", false
}

// GetGoType always returns false.
// There are no user-defined types in CDF
func (cdf *CDF) GetGoType(name string) (string, bool) {
	return "", false
}

// SetLogLevel sets the logging level to the given level, and returns
// the old level. This is for internal debugging use. The log messages
// are not expected to make much sense to anyone but the developers.
// The lowest level is 0 (no error logs at all) and the highest level is
// 3 (errors, warnings and debug messages).
func SetLogLevel(level int) int {
	old := logger.LogLevel()
	switch level {
	case 0:
		logger.SetLogLevel(internal.LevelFatal)
	case 1:
		logger.SetLogLevel(internal.LevelError)
	case 2:
		logger.SetLogLevel(internal.LevelWarn)
	default:
		logger.SetLogLevel(internal.LevelInfo)
	}
	return int(old)
}

func fail(message string, err error) {
	logger.Error(message)
	thrower.Throw(err)
}

func assert(condition bool, message string, err error) {
	if condition {
		return
	}
	fail(message, err)
}

// Read up to nBytes
func readBytes(r io.Reader, nBytes uint64) []byte {
	b := make([]byte, nBytes)
	err := binary.Read(r, binary.BigEndian, b)
	thrower.ThrowIfError(err)
	return b
}

func read8(r io.Reader) byte {
	var data byte
	err := binary.Read(r, binary.BigEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func read16(r io.Reader) uint16 {
	var data uint16
	err := binary.Read(r, binary.BigEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func read32(r io.Reader) uint32 {
	var data uint32
	err := binary.Read(r, binary.BigEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func read64(r io.Reader) uint64 {
	var data uint64
	err := binary.Read(r, binary.BigEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func seekTo(f io.Seeker, offset int64) {
	_, err := f.Seek(offset, io.SeekStart)
	thrower.ThrowIfError(err)
}

// V5 only
func (cdf *CDF) checkVersion(requiredVersion int) {
	assert(cdf.version >= uint8(requiredVersion),
		"invalid type for this file version",
		ErrCorruptedFile)
}

// Rounds up to next int boundary
func roundInt32(i uint64) uint64 {
	return (i + 3) & ^uint64(0x3)
}

func (cdf *CDF) getAttr(bf io.Reader) (string, interface{}) {
	name := cdf.readName(bf)
	vType := read32(bf)
	nvars := cdf.readNumber(bf)
	nread := uint64(0)
	var values interface{}
	switch vType {
	case typeByte:
		// byte becomes int8 in go-speak
		b := make([]int8, nvars)
		err := binary.Read(bf, binary.BigEndian, b)
		thrower.ThrowIfError(err)
		values = b
		nread += nvars

	case typeChar:
		// char array becomes string in go-speak
		b := make([]byte, nvars)
		err := binary.Read(bf, binary.BigEndian, b)
		thrower.ThrowIfError(err)
		values = string(b)
		nread += nvars

	case typeShort:
		sv := make([]int16, nvars)
		err := binary.Read(bf, binary.BigEndian, sv)
		thrower.ThrowIfError(err)
		values = sv
		nread += 2 * nvars

	case typeInt:
		i32v := make([]int32, nvars)
		err := binary.Read(bf, binary.BigEndian, i32v)
		thrower.ThrowIfError(err)
		values = i32v
		nread += 4 * nvars

	case typeFloat:
		fv := make([]float32, nvars)
		err := binary.Read(bf, binary.BigEndian, fv)
		thrower.ThrowIfError(err)
		values = fv
		nread += 4 * nvars

	case typeDouble:
		dv := make([]float64, nvars)
		err := binary.Read(bf, binary.BigEndian, dv)
		thrower.ThrowIfError(err)
		values = dv
		nread += 8 * nvars

	case typeUByte:
		cdf.checkVersion(5)
		// unsigned byte becomes uint8 in go-speak
		b := make([]uint8, nvars)
		for i := range b {
			b[i] = uint8(read8(bf))
		}
		values = b
		nread += nvars

	case typeUShort:
		cdf.checkVersion(5)
		sv := make([]uint16, nvars)
		for i := range sv {
			sv[i] = read16(bf)
		}
		values = sv
		nread += 2 * nvars

	case typeUInt:
		cdf.checkVersion(5)
		i32v := make([]uint32, nvars)
		for i := range i32v {
			i32v[i] = read32(bf)
		}
		values = i32v
		nread += 4 * nvars

	case typeInt64:
		cdf.checkVersion(5)
		i64v := make([]int64, nvars)
		for i := range i64v {
			i64v[i] = int64(read64(bf))
		}
		values = i64v
		nread += 8 * nvars

	case typeUInt64:
		cdf.checkVersion(5)
		i64v := make([]uint64, nvars)
		for i := range i64v {
			i64v[i] = read64(bf)
		}
		values = i64v
		nread += 8 * nvars

	default:
		fail(fmt.Sprint("corrupted file, unknown type: ", vType),
			ErrCorruptedFile)
	}
	// padding
	for nread&0x3 != 0 {
		_ = read8(bf)
		nread++
	}
	// If just one value in an attribute value slice, return it as a scalar
	val := reflect.ValueOf(values)
	if val.Kind() == reflect.Slice && val.Len() == 1 {
		values = val.Index(0).Interface()
	}
	return name, values
}

func (cdf *CDF) getNElems(bf io.Reader, expectedField uint32) uint64 {
	fieldType := read32(bf)
	nElems := cdf.readNumber(bf) // FYI: 64-bit in V5
	switch fieldType {
	case 0: // type absent
		assert(nElems == 0,
			fmt.Sprint("corrupted file, elems with absent field, expected: ",
				expectedField, nElems),
			ErrCorruptedFile)

	case expectedField:
		break
	default:
		fail(fmt.Sprint("corrupted file, unexpected field: ", fieldType),
			ErrCorruptedFile)
	}
	return nElems
}

func (cdf *CDF) getAttrList(bf io.Reader) *util.OrderedMap {
	nElems := cdf.getNElems(bf, fieldAttribute)
	attrs := make(map[string]interface{})
	keys := make([]string, 0)
	for i := uint64(0); i < nElems; i++ {
		name, val := cdf.getAttr(bf)
		attrs[name] = val
		keys = append(keys, name)
	}
	om, err := util.NewOrderedMap(keys, attrs)
	thrower.ThrowIfError(err)
	return om
}

func (cdf *CDF) readName(bf io.Reader) string {
	nameLen := cdf.readNumber(bf)
	b := readBytes(bf, roundInt32(nameLen))
	for i := uint64(0); i < nameLen; i++ {
		if b[i] == 0 {
			logger.Warnf("Null found in name %q %d %d version %d", string(b[:nameLen]), nameLen, i, cdf.version)
			nameLen = i
			break
		}
	}
	return string(b[:nameLen])
}

func (cdf *CDF) getDim(bf io.Reader) dimension {
	name := cdf.readName(bf)
	dimLength := cdf.readNumber(bf)
	return dimension{name, dimLength}
}

func (cdf *CDF) hasUnlimitedDimension(ids []uint64) bool {
	return len(ids) > 0 && cdf.dimensions[ids[0]].dimLength == 0
}

func (cdf *CDF) getVar(bf io.Reader) variable {
	name := cdf.readName(bf)
	nDims := cdf.readNumber(bf)
	assert(nDims <= maxDimensions,
		"too many dimensions",
		ErrTooManyDimensions)
	dimids := make([]uint64, nDims)
	for i := uint64(0); i < nDims; i++ {
		// dimid
		dimids[i] = cdf.readNumber(bf)
	}
	attrs := cdf.getAttrList(bf)
	vType := read32(bf)
	vsize := cdf.readNumber(bf)
	usedVsize := vsize
	// if unlimited, add vsize to current record size
	if nDims > 0 && cdf.hasUnlimitedDimension(dimids) {
		n := uint64(1)
		for i := 1; i < len(dimids); i++ {
			n *= cdf.dimensions[dimids[i]].dimLength
		}
		// Calculate the vsize actually used, without padding, which is
		// different than the vsize.  This matters when there is just one
		// record variable and it is small (special case).
		switch vType {
		case typeByte, typeUByte, typeChar:
			break
		case typeShort, typeUShort:
			n *= 2
		case typeInt, typeUInt, typeFloat:
			n *= 4
		case typeDouble, typeInt64, typeUInt64:
			n *= 8
		default:
			thrower.Throw(ErrUnknownType)
		}
		cdf.recSize += uint64(vsize)
		usedVsize = n
	}
	var offset uint64
	switch cdf.version {
	case 1:
		offset = uint64(read32(bf))
	case 2, 5:
		offset = read64(bf)
	default:
		thrower.Throw(ErrInternal)
	}
	return variable{name, dimids, attrs, vType, int64(usedVsize), offset}
}

func (cdf *CDF) readNumber(bf io.Reader) uint64 {
	if cdf.version < 5 {
		n := read32(bf)
		// Weird casts are to do sign extension
		return uint64(int64(int32(n)))
	}
	return read64(bf)
}

func (cdf *CDF) readHeader() (err error) {
	defer thrower.RecoverError(&err)
	bf := io.Reader(bufio.NewReader(cdf.file))

	// magic
	b := readBytes(bf, 4)
	if string(b[:3]) != "CDF" {
		logger.Infof("not cdf: %q", string(b[:3]))
		thrower.Throw(ErrNotCDF)
	}
	version := b[3]
	switch version {
	case 1, 2, 5: // classic, 64-bit offset, 64-bit types
		break

	default:
		fail(fmt.Sprint("unknown version: ", version),
			ErrUnknownVersion)
	}
	cdf.version = version
	// numrecs
	numRecs := cdf.readNumber(bf)
	assert(numRecs != 0xffffffffffffffff,
		"streaming not supported",
		ErrNoStreamingDimensions)
	cdf.numRecs = numRecs

	// dimlist
	nDims := cdf.getNElems(bf, fieldDimension)
	assert(nDims <= maxDimensions,
		"too many dimensions",
		ErrTooManyDimensions)
	if nDims > 0 {
		cdf.dimensions = make([]dimension, nDims)
		for i := uint64(0); i < nDims; i++ {
			cdf.dimensions[i] = cdf.getDim(bf)
		}
	}

	// gatt_list
	cdf.globalAttrs = cdf.getAttrList(bf)
	cdf.globalAttrs.Hide(ncpKey)
	// var list
	nVars := cdf.getNElems(bf, fieldVariable)

	nRecordVars := 0
	cdf.vars, err = util.NewOrderedMap(nil, nil)
	thrower.ThrowIfError(err)
	var firstRecordVar *variable
	for i := uint64(0); i < nVars; i++ {
		v := cdf.getVar(bf)
		_, has := cdf.vars.Get(v.name)
		if has {
			return ErrDuplicateVariable
		}
		if cdf.hasUnlimitedDimension(v.dimids) {
			nRecordVars++
			if firstRecordVar == nil {
				firstRecordVar = &v
			}
		}
		cdf.vars.Add(v.name, v)
	}
	switch nRecordVars {
	case 0:
		assert(cdf.recSize == 0,
			fmt.Sprint("No record variables, size should be zero: ", cdf.recSize),
			ErrInternal)
	case 1:
		firstLen := uint64(firstRecordVar.vsize)
		switch firstRecordVar.vType {
		case typeByte, typeUByte, typeChar:
			cdf.recSize = 1 * firstLen
			cdf.specialCase = true
		case typeShort, typeUShort:
			cdf.recSize = 2 * firstLen
			cdf.specialCase = true
		}
	default:
		newRecSize := roundInt32(cdf.recSize)
		if newRecSize != cdf.recSize {
			logger.Info("rounded recsize=", newRecSize, "orig=", cdf.recSize)
			cdf.recSize = newRecSize
		}
	}
	return nil
}

// Close closes this group and closes any underlying files if they are no
// longer being used by any other groups.
func (cdf *CDF) Close() {
	defer thrower.RecoverError(nil)
	assert(cdf.fileRefCount > 0, "ref count off", ErrInternal)
	cdf.fileRefCount--
	if cdf.fileRefCount == 0 {
		err := cdf.file.Close()
		if err != nil {
			logger.Error("Error on close (ignored):", err)
		}
		cdf.file = nil
	}
}

// GetGroup gets the given group or returns an error if not found.
// The group can start with "/" for absolute names, or relative.
func (cdf *CDF) GetGroup(group string) (g api.Group, err error) {
	// TODO: use hdf5 file refcounting
	// TODO: fake groups with "/" in names
	defer thrower.RecoverError(&err)
	if group != "" && group != "/" {
		return nil, ErrNotFound
	}
	cdf.fileRefCount++
	return api.Group(cdf), nil
}

// Open is the implementation of the API netcdf.Open.
// Using netcdf.Open is preferred over using this directly.
func Open(fname string) (api.Group, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	c, err := New(file)
	if err != nil {
		file.Close()
	}
	return c, err
}

// New is the implementation of the API netcdf.New.
// Using netcdf.New is preferred over using this directly.
func New(file api.ReadSeekerCloser) (ag api.Group, err error) {
	defer thrower.RecoverError(&err)
	c := &CDF{file: file, fileRefCount: 1}
	err = c.readHeader()
	if err != nil {
		return nil, err
	}
	if f, ok := file.(*os.File); ok {
		c.fname = f.Name()
	}
	return api.Group(c), nil
}

// Attributes returns the global attributes for this group.
func (cdf *CDF) Attributes() api.AttributeMap {
	return cdf.globalAttrs
}

func (cdf *CDF) getVarCommon(name string) (api.VarGetter, error) {
	vf, has := cdf.vars.Get(name)
	if !has {
		return nil, ErrNotFound
	}
	varFound := vf.(variable)
	dimLengths := make([]uint64, len(varFound.dimids))
	dimNames := make([]string, len(varFound.dimids))

	totalSize := int64(1)
	unlimited := false
	for i := range dimLengths {
		dimid := varFound.dimids[i]
		assert(dimid < uint64(len(cdf.dimensions)),
			fmt.Sprint(name, " dimid: ", varFound.dimids[i], " not found"),
			ErrInternal)

		dim := cdf.dimensions[dimid]
		dimLengths[i] = dim.dimLength
		// Handle unlimited dimension.
		if dimLengths[i] == 0 {
			assert(i == 0,
				"unlimited dimension must be first",
				ErrCorruptedFile)
			unlimited = true
			dimLengths[i] = cdf.numRecs
		}
		dimNames[i] = dim.name
		totalSize *= int64(dimLengths[i])
	}
	var chunkSize int64
	length := int64(0)
	switch {
	case len(dimLengths) == 0:
		chunkSize = totalSize
		length = 1
	case dimLengths[0] == 0:
		chunkSize = totalSize
		length = 0
	default:
		chunkSize = totalSize / int64(dimLengths[0])
		length = int64(dimLengths[0])
	}
	getSlice := func(begin, end int64) (interface{}, error) {
		if end < begin {
			return nil, errors.New("invalid slice parameters")
		}
		var nChunks int64
		switch {
		case len(dimLengths) == 0:
			// scalar
			nChunks = 1
		case dimLengths[0] == 0:
			// unlimited
			nChunks = 0
		default:
			nChunks = int64(end - begin)
		}
		sliceSize := int64(1)
		var start int64
		var sizeInBytes int64

		sliceSize = nChunks * chunkSize
		start = begin * chunkSize
		switch varFound.vType {
		case typeDouble, typeInt64, typeUInt64:
			start *= 8
			sizeInBytes = sliceSize * 8
		case typeInt, typeFloat, typeUInt:
			start *= 4
			sizeInBytes = sliceSize * 4
		case typeShort, typeUShort:
			start *= 2
			sizeInBytes = sliceSize * 2
		case typeChar, typeByte, typeUByte:
			sizeInBytes = sliceSize
		default:
			thrower.Throw(ErrInternal)
		}

		var bf io.Reader
		if unlimited && !cdf.specialCase {
			bf = cdf.newRecordReader(&varFound, start, sizeInBytes)
		} else {
			seekTo(cdf.file, int64(varFound.begin)+start)
			bf = io.LimitReader(makeFillValueReader(varFound,
				io.Reader(bufio.NewReader(cdf.file))), sizeInBytes)
		}

		// in case of unlimited, should read a record at a time, using cdf.recSize
		var data interface{}
		switch varFound.vType {
		case typeByte:
			data = make([]int8, sliceSize)

		case typeChar:
			data = make([]byte, sliceSize)

		case typeShort:
			data = make([]int16, sliceSize)

		case typeInt:
			data = make([]int32, sliceSize)

		case typeFloat:
			data = make([]float32, sliceSize)

		case typeDouble:
			data = make([]float64, sliceSize)

		case typeUByte:
			data = make([]uint8, sliceSize)

		case typeUShort:
			data = make([]uint16, sliceSize)

		case typeUInt:
			data = make([]uint32, sliceSize)

		case typeUInt64:
			data = make([]uint64, sliceSize)

		case typeInt64:
			data = make([]int64, sliceSize)

		default:
			fail("unknown type", ErrUnknownType)
		}
		err := binary.Read(bf, binary.BigEndian, data)
		if err != nil {
			return nil, err
		}
		dimLengthsCopy := make([]uint64, len(dimLengths))
		copy(dimLengthsCopy, dimLengths)
		if len(dimLengthsCopy) > 0 {
			dimLengthsCopy[0] = uint64(nChunks)
		}
		converted := cdf.convert(data, dimLengthsCopy, varFound.vType)
		if converted == nil {
			thrower.Throw(ErrInternal)
		}
		return converted, nil
	}
	return internal.NewSlicer(getSlice, length, dimNames, varFound.attrs,
		cdlType(varFound.vType), goType(varFound.vType)), nil
}

func goType(vType uint32) string {
	switch vType {
	case typeByte:
		return "int8"

	case typeChar:
		return "string"

	case typeShort:
		return "int16"

	case typeInt:
		return "int32"

	case typeFloat:
		return "float32"

	case typeDouble:
		return "float64"

	case typeUByte:
		return "uint8"

	case typeUShort:
		return "uint16"

	case typeUInt:
		return "uint32"

	case typeUInt64:
		return "uint64"

	case typeInt64:
		return "int64"

	default:
		fail("unknown type", ErrUnknownType)
	}
	panic("never gets here")
}

func cdlType(vType uint32) string {
	switch vType {
	case typeByte:
		return "byte"

	case typeChar:
		return "char"

	case typeShort:
		return "short"

	case typeInt:
		return "int"

	case typeFloat:
		return "float"

	case typeDouble:
		return "double"

	case typeUByte:
		return "ubyte"

	case typeUShort:
		return "ushort"

	case typeUInt:
		return "uint"

	case typeUInt64:
		return "uint64"

	case typeInt64:
		return "int64"

	default:
		fail("unknown type", ErrUnknownType)
	}
	panic("never gets here")
}

// GetVarGetter is an function that returns an interface that allows you to get
// smaller slices of a variable, in case the variable is very large and you want to
// reduce memory usage.
func (cdf *CDF) GetVarGetter(name string) (slicer api.VarGetter, err error) {
	defer thrower.RecoverError(&err)
	return cdf.getVarCommon(name)
}

// GetVariable returns the named variable or sets the error if not found.
func (cdf *CDF) GetVariable(name string) (v *api.Variable, err error) {
	defer thrower.RecoverError(&err)
	sl, err := cdf.getVarCommon(name)
	if err != nil {
		return nil, err
	}
	vals, err := sl.GetSlice(0, sl.Len())
	if err != nil {
		return nil, err
	}
	return &api.Variable{
		Values:     vals,
		Dimensions: sl.Dimensions(),
		Attributes: sl.Attributes()}, nil
}

// Seeks and read bytes
type seekReader struct {
	file   io.ReadSeeker
	offset int64
	reader io.Reader
}

func (sr *seekReader) Read(p []byte) (int, error) {
	if sr.reader == nil {
		seekTo(sr.file, sr.offset)
		sr.reader = bufio.NewReader(sr.file)
	}
	return sr.reader.Read(p)
}

func newSeekReader(file io.ReadSeeker, offset int64) io.Reader {
	return &seekReader{file: file, offset: offset, reader: nil}
}

func (cdf *CDF) newRecordReader(v *variable, start int64, size int64) io.Reader {
	readers := make([]io.Reader, cdf.numRecs)

	for i := uint64(0); i < uint64(cdf.numRecs) && size > 0; i++ {
		increment := int64(i) * int64(cdf.recSize)
		if start > increment {
			continue
		}
		offset := int64(v.begin) + increment
		begin := offset + start
		thisSize := v.vsize
		readers[i] = io.LimitReader(newSeekReader(cdf.file, begin), thisSize)
		size -= thisSize
		start = 0
	}
	return io.MultiReader(readers...)
}

func emptySlice(v interface{}, dimLengths []uint64) interface{} {
	top := reflect.ValueOf(v)
	elemType := top.Type().Elem()
	var empty reflect.Value
	for i := 0; i < len(dimLengths); i++ {
		empty = reflect.MakeSlice(reflect.SliceOf(elemType), 0, 0)
		elemType = empty.Type()
	}
	return empty.Interface()
}

func (cdf *CDF) convert(data interface{}, dimLengths []uint64, vType uint32) interface{} {
	// in case of unlimited, should read a record at a time, using cdf.recSize

	// try fast conversions first
	if !cdf.slowConvert && len(dimLengths) == 0 {
		switch vType {
		case typeByte:
			v := data.([]int8)
			return v[0]

		case typeChar:
			v := data.([]byte)
			return string(v[0])

		case typeShort:
			v := data.([]int16)
			return v[0]

		case typeInt:
			v := data.([]int32)
			return v[0]

		case typeFloat:
			v := data.([]float32)
			return v[0]

		case typeDouble:
			v := data.([]float64)
			return v[0]

		case typeUByte:
			v := data.([]uint8)
			return v[0]

		case typeUShort:
			v := data.([]uint16)
			return v[0]

		case typeUInt:
			v := data.([]uint32)
			return v[0]

		case typeUInt64:
			v := data.([]uint64)
			return v[0]

		case typeInt64:
			v := data.([]int64)
			return v[0]

		default:
			fail("unknown type", ErrUnknownType)
		}
	}
	if !cdf.slowConvert && len(dimLengths) == 1 {
		switch vType {
		case typeChar:
			return string(data.([]byte))
		default:
			return data
		}
	}
	if !cdf.slowConvert && len(dimLengths) == 2 {
		switch vType {
		case typeByte:
			v := data.([]int8)
			ret := make([][]int8, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeChar:
			v := data.([]byte)
			ret := make([]string, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = string(v[:dimLengths[1]])
				v = v[dimLengths[1]:]
			}
			return ret

		case typeShort:
			v := data.([]int16)
			ret := make([][]int16, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeInt:
			v := data.([]int32)
			ret := make([][]int32, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeFloat:
			v := data.([]float32)
			ret := make([][]float32, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeDouble:
			v := data.([]float64)
			ret := make([][]float64, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeUByte:
			v := data.([]uint8)
			ret := make([][]uint8, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeUShort:
			v := data.([]uint16)
			ret := make([][]uint16, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeUInt:
			v := data.([]uint32)
			ret := make([][]uint32, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeUInt64:
			v := data.([]uint64)
			ret := make([][]uint64, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		case typeInt64:
			v := data.([]int64)
			ret := make([][]int64, dimLengths[0])
			for i := 0; i < int(dimLengths[0]); i++ {
				ret[i] = v[:dimLengths[1]]
				v = v[dimLengths[1]:]
			}
			return ret

		default:
			fail("unknown type", ErrUnknownType)
		}
	}

	v := reflect.ValueOf(data)
	if v.Len() == 0 {
		return emptySlice(data, dimLengths)
	}
	if len(dimLengths) == 0 {
		elem := v.Index(0)
		if vType == typeChar {
			return string([]byte{elem.Interface().(byte)})
		}
		return elem.Interface()
	}
	length := int(dimLengths[0])
	if len(dimLengths) == 1 {
		returnSlice := v.Slice(0, int(dimLengths[0])).Interface()
		if vType == typeChar {
			// convert to string
			b := returnSlice.([]byte)
			return string(b)
		}
		return returnSlice
	}

	ivalue := cdf.convert(data, dimLengths[1:], vType)
	t := reflect.TypeOf(ivalue)
	val := reflect.MakeSlice(reflect.SliceOf(t), length, length)
	val.Index(0).Set(reflect.ValueOf(ivalue))
	for i := 1; i < length; i++ {
		nextSlice := v.Slice(i*int(dimLengths[1]), v.Len()).Interface()
		data = nextSlice
		ivalue = cdf.convert(data, dimLengths[1:], vType)
		val.Index(i).Set(reflect.ValueOf(ivalue))
	}
	return val.Interface()
}

func makeFillValueShort(s int16) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, &s)
	thrower.ThrowIfError(err)
	return buf.Bytes()
}

func makeFillValueInt(i int32) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, &i)
	thrower.ThrowIfError(err)
	return buf.Bytes()
}

func makeFillValueInt64(i64 int64) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, &i64)
	thrower.ThrowIfError(err)
	return buf.Bytes()
}

func makeFillValueFloat(f float32) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, &f)
	thrower.ThrowIfError(err)
	return buf.Bytes()
}

func makeFillValueDouble(d float64) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, &d)
	thrower.ThrowIfError(err)
	return buf.Bytes()
}

func makeFillValueReader(v variable, bf io.Reader) io.Reader {
	userFV, hasUserFV := v.attrs.Get("_FillValue")
	if hasUserFV {
		val := reflect.ValueOf(userFV)
		if val.Kind() == reflect.Slice {
			if val.Len() != 1 {
				thrower.Throw(ErrFillValue)
			}
			userFV = val.Index(0).Interface()
		}
	}
	var fillValue []byte
	switch v.vType {
	case typeFloat:
		fv := math.Float32frombits(0x7cf00000)
		if hasUserFV {
			fv = userFV.(float32)
		}
		fillValue = makeFillValueFloat(fv)

	case typeDouble:
		fv := math.Float64frombits(0x479e000000000000)
		if hasUserFV {
			fv = userFV.(float64)
		}
		fillValue = makeFillValueDouble(fv)

	case typeByte:
		fv := byte(0x81)
		if hasUserFV {
			fv = byte(userFV.(int8))
		}
		fillValue = []byte{fv}

	case typeUByte:
		fv := byte(0xff)
		if hasUserFV {
			fv = byte(userFV.(uint8))
		}
		fillValue = []byte{fv}

	case typeShort:
		fv := uint16(0x8001)
		if hasUserFV {
			fv = uint16(userFV.(int16))
		}
		fillValue = makeFillValueShort(int16(fv))

	case typeUShort:
		fv := uint16(0xffff)
		if hasUserFV {
			fv = userFV.(uint16)
		}
		fillValue = makeFillValueShort(int16(fv))

	case typeInt:
		fv := uint32(0x80000001)
		if hasUserFV {
			fv = uint32(userFV.(int32))
		}
		fillValue = makeFillValueInt(int32(fv))

	case typeUInt:
		fv := uint32(0xffffffff)
		if hasUserFV {
			fv = userFV.(uint32)
		}
		fillValue = makeFillValueInt(int32(fv))

	case typeInt64:
		fv := uint64(0x8000000000000002)
		if hasUserFV {
			fv = uint64(userFV.(int64))
		}
		fillValue = makeFillValueInt64(int64(fv))

	case typeUInt64:
		fv := uint64(0xfffffffffffffffe)
		if hasUserFV {
			fv = userFV.(uint64)
		}
		fillValue = makeFillValueInt64(int64(fv))

	case typeChar:
		fv := byte(0x00)
		if hasUserFV {
			fv = userFV.(byte)
		}
		fillValue = []byte{fv}

	default:
		thrower.Throw(ErrInternal)
	}
	return io.MultiReader(bf, internal.NewFillValueReader(fillValue))
}
