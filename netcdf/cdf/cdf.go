// Package CDF supports v1 (classic), v2 (64-bit offset) and v5 file formats.
package cdf

// TODO: api for dimensions in case of unlimited
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

type CDF struct {
	fname        string
	file         io.ReadSeeker
	fileRefCount int
	version      uint8
	numRecs      uint64 // 64-bits in V5
	recSize      uint64
	dimensions   []dimension
	globalAttrs  *util.OrderedMap
	vars         *util.OrderedMap
	specialCase  bool
}

const maxDimensions = 1024

var (
	ErrNotCDF                = errors.New("Not a CDF file")
	ErrUnsupportedVersion    = errors.New("Unsupported CDF version")
	ErrUnknownVersion        = errors.New("Unknown CDF version")
	ErrUnknownType           = errors.New("Unknown type")
	ErrCorruptedFile         = errors.New("Corrupted file")
	ErrNotFound              = errors.New("Not found")
	ErrNoStreamingDimensions = errors.New("Streaming dimensions not supported")
	ErrInternal              = errors.New("Internal error")
	ErrDuplicateVariable     = errors.New("Duplicate variable")
	ErrTooManyDimensions     = errors.New("Too many dimensions")
	ErrFillValue             = errors.New("Fill value not a scalar")
)

var (
	logger = util.NewLogger()
)

func (cdf *CDF) ListVariables() []string {
	return cdf.vars.Keys()
}

func (cdf *CDF) ListSubgroups() []string {
	return nil
}

func SetLogLevel(level int) {
	logger.SetLogLevel(level)
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
	_, err := f.Seek(offset, os.SEEK_SET)
	thrower.ThrowIfError(err)
}

// V5 only
func (cdf *CDF) checkVersion(requiredVersion int) {
	if cdf.version < uint8(requiredVersion) {
		logger.Error("invalid type for this file version")
		thrower.Throw(ErrCorruptedFile)
	}
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
		logger.Error("corrupted file, unknown type:", vType)
		thrower.Throw(ErrCorruptedFile)
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
		if nElems != 0 {
			logger.Error("corrupted file, elems with absent field, expected:", expectedField, nElems)
			thrower.Throw(ErrCorruptedFile)
		}
	case expectedField:
		break
	default:
		logger.Error("corrupted file, unexpected field:", fieldType)
		thrower.Throw(ErrCorruptedFile)
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
	if err != nil {
		thrower.Throw(err)
	}
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
	if nDims > maxDimensions {
		thrower.Throw(ErrTooManyDimensions)
	}
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
		logger.Warn("not cdf: ", fmt.Sprintf("%q", string(b[:3])))
		return ErrNotCDF
	}
	version := b[3]
	switch version {
	case 1, 2, 5: // classic, 64-bit offset, 64-bit types
		break

	default:
		logger.Error("unknown version:", version)
		return ErrUnknownVersion
	}
	cdf.version = version

	// numrecs
	numRecs := cdf.readNumber(bf)
	if numRecs == 0xffffffffffffffff {
		logger.Warn("streaming not supported")
		return ErrNoStreamingDimensions
	}
	cdf.numRecs = numRecs

	// dimlist
	nDims := cdf.getNElems(bf, fieldDimension)
	if nDims > maxDimensions {
		thrower.Throw(ErrTooManyDimensions)
	}
	if nDims > 0 {
		cdf.dimensions = make([]dimension, nDims)
		for i := uint64(0); i < nDims; i++ {
			cdf.dimensions[i] = cdf.getDim(bf)
		}
	}

	// gatt_list
	cdf.globalAttrs = cdf.getAttrList(bf)

	// var list
	nVars := cdf.getNElems(bf, fieldVariable)
	if nVars == 0 {
		return nil
	}

	nRecordVars := 0
	cdf.vars, err = util.NewOrderedMap(nil, nil)
	if err != nil {
		thrower.Throw(err)
	}
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
		if cdf.recSize != 0 {
			logger.Error("No record variables, size should be zero", cdf.recSize)
			thrower.Throw(ErrInternal)
		}
		break
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

// Close the CDF & release resources.
//
// Close will close the underlying reader if it implements io.Closer.
func (cdf *CDF) Close() {
	if cdf.fileRefCount <= 0 {
		panic("refcount issue")
	}
	cdf.fileRefCount--
	if cdf.fileRefCount == 0 {
		if f, ok := cdf.file.(io.Closer); ok {
			f.Close()
		}
		cdf.file = nil
	}
}

// TODO: use hdf5 file refcounting
// TODO: fake groups with "/" in names
func (cdf *CDF) GetGroup(group string) (g api.Group, err error) {
	if group != "" && group != "/" {
		return nil, ErrNotFound
	}
	cdf.fileRefCount++
	return api.Group(cdf), nil
}

func NewCDF(fname string) (api.Group, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	c, err := NewFrom(file)
	if err != nil {
		file.Close()
	}
	return c, err
}

func NewFrom(file io.ReadSeeker) (api.Group, error) {
	c := &CDF{file: file, fileRefCount: 1}
	err := c.readHeader()
	if err != nil {
		return nil, err
	}
	if f, ok := file.(*os.File); ok {
		c.fname = f.Name()
	}
	return api.Group(c), nil
}

func (cdf *CDF) Attributes() api.AttributeMap {
	return cdf.globalAttrs
}

func (cdf *CDF) GetVariable(name string) (v *api.Variable, err error) {
	defer thrower.RecoverError(&err)
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
		if dimid >= uint64(len(cdf.dimensions)) {
			logger.Error(name, "dimid", varFound.dimids[i], "not found")
			thrower.Throw(ErrInternal)
		}
		dim := cdf.dimensions[dimid]
		dimLengths[i] = dim.dimLength
		// Handle unlimited dimension.
		if dimLengths[i] == 0 {
			if !unlimited && i != 0 {
				logger.Error("unlimited dimension must be first")
				thrower.Throw(ErrCorruptedFile)
			}
			unlimited = true
			dimLengths[i] = cdf.numRecs
		}
		dimNames[i] = dim.name
		totalSize *= int64(dimLengths[i])
	}
	var sizeInBytes int64
	switch varFound.vType {
	case typeDouble, typeInt64, typeUInt64:
		sizeInBytes = totalSize * 8
	case typeInt, typeFloat, typeUInt:
		sizeInBytes = totalSize * 4
	case typeShort, typeUShort:
		sizeInBytes = totalSize * 2
	case typeChar, typeByte, typeUByte:
		sizeInBytes = totalSize
	default:
		thrower.Throw(ErrInternal)
	}
	var bf io.Reader
	if unlimited && !cdf.specialCase {
		bf = cdf.newRecordReader(&varFound)
	} else {
		seekTo(cdf.file, int64(varFound.begin))
		bf = io.LimitReader(makeFillValueReader(varFound,
			io.Reader(bufio.NewReader(cdf.file))), sizeInBytes)
	}

	// in case of unlimited, should read a record at a time, using cdf.recSize
	var data interface{}
	switch varFound.vType {
	case typeByte:
		data = make([]int8, totalSize)

	case typeChar:
		data = make([]byte, totalSize)

	case typeShort:
		data = make([]int16, totalSize)

	case typeInt:
		data = make([]int32, totalSize)

	case typeFloat:
		data = make([]float32, totalSize)

	case typeDouble:
		data = make([]float64, totalSize)

	case typeUByte:
		data = make([]uint8, totalSize)

	case typeUShort:
		data = make([]uint16, totalSize)

	case typeUInt:
		data = make([]uint32, totalSize)

	case typeUInt64:
		data = make([]uint64, totalSize)

	case typeInt64:
		data = make([]int64, totalSize)

	default:
		logger.Error("unknown type")
		thrower.Throw(ErrUnknownType)
	}
	err = binary.Read(bf, binary.BigEndian, data)
	thrower.ThrowIfError(err)
	// TODO: it should be possible to avoid this conversion and use
	// reflection inline.
	converted, _ := convert(data, dimLengths, varFound.vType)
	if converted == nil {
		thrower.Throw(ErrInternal)
	}
	return &api.Variable{converted, dimNames, varFound.attrs}, nil
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

func (cdf *CDF) newRecordReader(v *variable) io.Reader {
	readers := make([]io.Reader, cdf.numRecs)
	for i := uint64(0); i < cdf.numRecs; i++ {
		offset := int64(v.begin) + int64(i)*int64(cdf.recSize)
		readers[i] = io.LimitReader(newSeekReader(cdf.file, offset), v.vsize)
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

func convert(data interface{}, dimLengths []uint64, vType uint32) (interface{}, interface{}) {
	v := reflect.ValueOf(data)
	if len(dimLengths) == 0 {
		// we parsed an array, convert to scalar
		elem := v.Index(0)
		if vType == typeChar {
			return string([]byte{elem.Interface().(byte)}), nil
		}
		return elem.Interface(), nil
	}
	if v.Len() == 0 {
		return emptySlice(data, dimLengths), nil
	}
	if len(dimLengths) == 1 {
		returnSlice := v.Slice(0, int(dimLengths[0])).Interface()
		nextSlice := v.Slice(int(dimLengths[0]), v.Len()).Interface()
		if vType == typeChar {
			// convert to string
			b := returnSlice.([]byte)
			return string(b), nextSlice
		}
		return returnSlice, nextSlice
	}

	length := int(dimLengths[0])
	ivalue, data := convert(data, dimLengths[1:], vType)
	t := reflect.TypeOf(ivalue)
	val := reflect.MakeSlice(reflect.SliceOf(t), length, length)
	val.Index(0).Set(reflect.ValueOf(ivalue))
	for i := 1; i < length; i++ {
		ivalue, data = convert(data, dimLengths[1:], vType)
		val.Index(i).Set(reflect.ValueOf(ivalue))
	}
	return val.Interface(), data
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
	return io.MultiReader(bf, util.NewFillValueReader(fillValue))
}
