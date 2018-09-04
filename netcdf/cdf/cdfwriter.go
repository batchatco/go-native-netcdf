package cdf

// TODO: write unlimited dimension
// TODO: too many dimensions error
// TODO: api for dimensions in case of unlimited and zero length
import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/thrower"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

type countedWriter struct {
	w     *bufio.Writer
	count int64
}

type savedVar struct {
	name       string
	val        interface{}
	ty         int
	dimLengths []int64
	dimNames   []string
	attrs      api.AttributeMap
	offset     int64
}

type CDFWriter struct {
	file        *os.File
	bf          *countedWriter
	vars        []savedVar
	globalAttrs api.AttributeMap
	dimLengths  map[string]int64
	dimNames    []string
	dimIds      map[string]int64
	nextId      int64
	version     int8
}

type char byte

var (
	ErrUnlimitedMustBeFirst = errors.New("Unlimited dimension must be first")
	ErrEmptySlice           = errors.New("Empty slice encountered")
	ErrDimensionSize        = errors.New("Dimension doesn't match size")
	ErrInvalidName          = errors.New("Invalid name")
)

func (c *countedWriter) Count() int64 {
	return c.count
}

func (c *countedWriter) Flush() error {
	return c.w.Flush()
}

func (c *countedWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.count += int64(n)
	return n, err
}

func (cw *CDFWriter) storeChars(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		// a single character
		str := val.String()
		if len(str) == 0 {
			return
		}
		write8(cw.bf, int8(str[0]))
		return
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		// a string which must be padded
		s, ok := val.Interface().(string)
		if !ok {
			thrower.Throw(ErrInternal)
		}
		// TODO: this is probably wrong because it could be unlimited
		writeBytes(cw.bf, []byte(s))
		offset := int64(val.Len())
		for offset < thisDim {
			write8(cw.bf, 0)
			offset++
		}
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeChars(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeBytes(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write8(cw.bf, int8(val.Int()))
		return
	}
	if len(dimLengths) == 1 {
		b := val.Interface().([]int8)
		writeAny(cw.bf, b)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeBytes(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeUBytes(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write8(cw.bf, int8(val.Uint()))
		return
	}
	if len(dimLengths) == 1 {
		b := val.Interface().([]uint8)
		writeAny(cw.bf, b)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeUBytes(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeShorts(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write16(cw.bf, int16(val.Int()))
		return
	}
	if len(dimLengths) == 1 {
		s := val.Interface().([]int16)
		writeAny(cw.bf, s)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeShorts(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeUShorts(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write16(cw.bf, int16(val.Uint()))
		return
	}
	if len(dimLengths) == 1 {
		s := val.Interface().([]uint16)
		writeAny(cw.bf, s)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeUShorts(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeInts(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write32(cw.bf, int32(val.Int()))
		return
	}
	if len(dimLengths) == 1 {
		iv := val.Interface().([]int32)
		writeAny(cw.bf, iv)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeInts(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeUInts(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write32(cw.bf, int32(val.Uint()))
		return
	}
	if len(dimLengths) == 1 {
		iv := val.Interface().([]uint32)
		writeAny(cw.bf, iv)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeUInts(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeInt64s(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write64(cw.bf, val.Int())
		return
	}
	if len(dimLengths) == 1 {
		iv := val.Interface().([]int64)
		writeAny(cw.bf, iv)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeInt64s(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeUInt64s(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		write64(cw.bf, int64(val.Uint()))
		return
	}
	if len(dimLengths) == 1 {
		iv := val.Interface().([]uint64)
		writeAny(cw.bf, iv)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeUInt64s(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeFloats(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		f := float32(val.Float())
		i := math.Float32bits(f)
		write32(cw.bf, int32(i))
		return
	}
	if len(dimLengths) == 1 {
		fv := val.Interface().([]float32)
		writeAny(cw.bf, fv)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeFloats(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) storeDoubles(val reflect.Value, dimLengths []int64) {
	if len(dimLengths) == 0 {
		f := val.Float()
		i := math.Float64bits(f)
		write64(cw.bf, int64(i))
		return
	}
	if len(dimLengths) == 1 {
		dv := val.Interface().([]float64)
		writeAny(cw.bf, dv)
		return
	}
	for i := 0; i < val.Len(); i++ {
		value := val.Index(int(i))
		cw.storeDoubles(value, dimLengths[1:])
	}
}

func (cw *CDFWriter) getDimLengthsHelper(
	rv reflect.Value,
	dims []int64,
	dimNames []string) ([]int64, int) {

	t := rv.Type()
	kind := cw.scalarKind(t.Kind())
	switch kind {
	case typeNone:
		break
	case typeChar:
		if rv.Len() == 1 && len(dimNames) == 0 {
			return dims, kind
		}
	default:
		return dims, kind
	}
	vLen := int64(rv.Len())
	if vLen == 0 {
		thrower.Throw(ErrEmptySlice)
	}
	dims = append(dims, vLen)
	switch t.Kind() {
	case reflect.Array, reflect.Slice:
		if t.Elem().Kind() == reflect.String {
			// special case to longest string
			maxLen := int64(0)
			for i := 0; i < int(vLen); i++ {
				if int64(rv.Index(i).Len()) > maxLen {
					maxLen = int64(rv.Index(i).Len())
				}
			}
			if maxLen == 0 {
				thrower.Throw(ErrEmptySlice)
			}
			dims = append(dims, maxLen)
			return dims, typeChar
		}
		return cw.getDimLengthsHelper(rv.Index(0), dims, dimNames)

	case reflect.String:
		return dims, typeChar // we will make this an array of characters (bytes)
	}

	logger.Info("Unknown type", t.Kind())
	thrower.Throw(ErrUnknownType)
	panic("internal error") // should never happen
}

func (cw *CDFWriter) getDimLengths(val interface{}, dimNames []string) ([]int64, int) {
	v := reflect.ValueOf(val)
	dims := make([]int64, 0)
	return cw.getDimLengthsHelper(v, dims, dimNames)
}

func (cw *CDFWriter) scalarKind(goKind reflect.Kind) int {
	switch goKind {

	case reflect.String:
		return typeChar // This may not be a scalar: must do other checks

	case reflect.Int8:
		return typeByte

	case reflect.Int16:
		return typeShort

	case reflect.Int32:
		return typeInt

	case reflect.Float32:
		return typeFloat

	case reflect.Float64:
		return typeDouble

		// v5
	case reflect.Uint8:
		cw.version = 5
		return typeUByte

	case reflect.Uint16:
		cw.version = 5
		return typeUShort

	case reflect.Uint32:
		cw.version = 5
		return typeUInt

	case reflect.Uint64:
		cw.version = 5
		return typeUInt64

	case reflect.Int64:
		cw.version = 5
		return typeInt64

	}
	// not a scalar
	return typeNone
}

func hasValidNames(am api.AttributeMap) bool {
	if am == nil {
		return true
	}
	for _, key := range am.Keys() {
		if !util.IsValidNetCDFName(key) {
			return false
		}
	}
	return true
}

func (cw *CDFWriter) AddGlobalAttrs(attrs api.AttributeMap) error {
	if !hasValidNames(attrs) {
		return ErrInvalidName
	}
	cw.globalAttrs = attrs
	return nil
}

func (cw *CDFWriter) AddVar(name string, vr api.Variable) (err error) {
	defer thrower.RecoverError(&err)

	if !util.IsValidNetCDFName(name) {
		return ErrInvalidName
	}
	if !hasValidNames(vr.Attributes) {
		return ErrInvalidName
	}
	// TODO: check name for validity
	cw.checkV5Attributes(vr.Attributes)
	dimLengths, ty := cw.getDimLengths(vr.Values, vr.Dimensions)
	for i := 0; i < len(dimLengths); i++ {
		var dimName string
		if i < len(vr.Dimensions) {
			dimName = vr.Dimensions[i]
		}
		if dimName == "" {
			if ty == typeChar && i == len(dimLengths)-1 {
				dimName = fmt.Sprintf("_stringlen_%s", name)
			} else {
				dimName = fmt.Sprintf("_dimid_%d", cw.nextId)
			}
			vr.Dimensions = append(vr.Dimensions, dimName)
		}

		dimName = vr.Dimensions[i]
		currentLength, has := cw.dimLengths[dimName]
		if has {
			if dimLengths[i] != currentLength {
				thrower.Throw(ErrDimensionSize)
			}
		} else {
			cw.dimLengths[dimName] = dimLengths[i]
			cw.dimIds[dimName] = cw.nextId
			cw.dimNames = append(cw.dimNames, dimName)
			cw.nextId++
		}
	}
	cw.vars = append(cw.vars, savedVar{name, vr.Values, ty, dimLengths,
		vr.Dimensions, vr.Attributes, 0})
	return nil
}

func (cw *CDFWriter) writeAttributes(attrs api.AttributeMap) {
	if attrs == nil || len(attrs.Keys()) == 0 {
		write32(cw.bf, 0)        //  attributes: absent
		cw.writeNumber(int64(0)) // attributes: absent
		return
	}
	write32(cw.bf, fieldAttribute)
	cw.writeNumber(int64(len(attrs.Keys())))
	for _, k := range attrs.Keys() {
		v, _ := attrs.Get(k)
		cw.writeName(k)
		switch v.(type) {
		case string:
			write32(cw.bf, typeChar)
			vals := v.(string)
			cw.writeNumber(int64(len(vals)))
			writeBytes(cw.bf, []byte(vals))
			cw.pad()

		case []int8, int8:
			write32(cw.bf, typeByte)
			vals, ok := v.([]int8)
			if !ok {
				vals = []int8{v.(int8)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)
			cw.pad()
		case []int16, int16:
			write32(cw.bf, typeShort)
			vals, ok := v.([]int16)
			if !ok {
				vals = []int16{v.(int16)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)
			cw.pad()

		case []int32, int32:
			write32(cw.bf, typeInt)
			vals, ok := v.([]int32)
			if !ok {
				vals = []int32{v.(int32)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)

		case []float32, float32:
			write32(cw.bf, typeFloat)
			vals, ok := v.([]float32)
			if !ok {
				vals = []float32{v.(float32)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)

		case []float64, float64:
			write32(cw.bf, typeDouble)
			vals, ok := v.([]float64)
			if !ok {
				vals = []float64{v.(float64)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)

			// v5
		case []uint8, uint8: // []uint8 and []byte are the same thing
			write32(cw.bf, typeUByte)
			vals, ok := v.([]uint8)
			if !ok {
				vals = []uint8{v.(uint8)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)
			cw.pad()

		case []uint16, uint16:
			write32(cw.bf, typeUShort)
			vals, ok := v.([]uint16)
			if !ok {
				vals = []uint16{v.(uint16)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)
			cw.pad()

		case []uint32, uint32:
			write32(cw.bf, typeUInt)
			vals, ok := v.([]uint32)
			if !ok {
				vals = []uint32{v.(uint32)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)

		case []int64, int64:
			write32(cw.bf, typeInt64)
			vals, ok := v.([]int64)
			if !ok {
				vals = []int64{v.(int64)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)

		case []uint64, uint64:
			write32(cw.bf, typeUInt64)
			vals, ok := v.([]uint64)
			if !ok {
				vals = []uint64{v.(uint64)}
			}
			cw.writeNumber(int64(len(vals)))
			writeAny(cw.bf, vals)

		default:
			logger.Warnf("Unknown type %T, %#v=%#v", v, k, v)
			thrower.Throw(ErrUnknownType)
		}
	}
}

func (cw *CDFWriter) checkV5Attributes(attrs api.AttributeMap) {
	if attrs == nil {
		return
	}
	for _, k := range attrs.Keys() {
		v, _ := attrs.Get(k)
		switch v.(type) {
		case string:
			return

		case []uint64, uint64, []int64, int64, []uint8, uint8, []uint16, uint16, []uint32, uint32:
			cw.version = 5
		}
	}
}

func (cw *CDFWriter) writeVar(saved *savedVar) {
	cw.writeName(saved.name)
	cw.writeNumber(int64(len(saved.dimLengths)))
	for i := range saved.dimLengths {
		name := saved.dimNames[i]
		dimId := cw.dimIds[name]
		cw.writeNumber(int64(dimId))
	}
	cw.writeAttributes(saved.attrs)

	write32(cw.bf, int32(saved.ty))
	vsize := int64(0)
	switch saved.ty {
	case typeDouble, typeInt64, typeUInt64:
		vsize = 8
	case typeInt, typeFloat, typeUInt:
		vsize = 4
	case typeShort, typeUShort:
		vsize = 2
	case typeChar, typeByte, typeUByte:
		vsize = 1
	default:
		thrower.Throw(ErrInternal)
	}
	for i, v := range saved.dimLengths {
		if v != 0 {
			vsize *= v
			continue
		}
		if i != 0 {
			logger.Error(saved.name, "dimension", i, "name", saved.dimNames[i],
				"has length 0")
			thrower.Throw(ErrUnlimitedMustBeFirst)
		}
	}
	// pad vsize
	vsize = 4 * ((vsize + 3) / 4)
	cw.writeNumber(int64(vsize))
	saved.offset = cw.bf.Count()
	write64(cw.bf, 0) // patch later
}

func roundInt64(i int64) int64 {
	return (i + 3) & ^0x3
}

func (cw *CDFWriter) pad() {
	offset := cw.bf.Count()
	extra := roundInt64(offset) - offset
	if extra > 0 {
		zero := [3]byte{}
		writeBytes(cw.bf, zero[:extra])
	}
}

func (cw *CDFWriter) writeData(saved savedVar) {
	// this has to be written later, not in the header
	offset := cw.bf.Count()
	// patch saved.offset with this offset
	switch saved.ty {
	case typeByte:
		cw.storeBytes(reflect.ValueOf(saved.val), saved.dimLengths)
		cw.pad()
	case typeChar: // char in CDF is string/byte in Go
		cw.storeChars(reflect.ValueOf(saved.val), saved.dimLengths)
		cw.pad()
	case typeShort:
		cw.storeShorts(reflect.ValueOf(saved.val), saved.dimLengths)
		cw.pad()
	case typeInt:
		cw.storeInts(reflect.ValueOf(saved.val), saved.dimLengths)
	case typeFloat:
		cw.storeFloats(reflect.ValueOf(saved.val), saved.dimLengths)
	case typeDouble:
		cw.storeDoubles(reflect.ValueOf(saved.val), saved.dimLengths)
	case typeInt64:
		cw.storeInt64s(reflect.ValueOf(saved.val), saved.dimLengths)
	case typeUInt64:
		cw.storeUInt64s(reflect.ValueOf(saved.val), saved.dimLengths)
	case typeUInt:
		cw.storeUInts(reflect.ValueOf(saved.val), saved.dimLengths)
	case typeUShort:
		cw.storeUShorts(reflect.ValueOf(saved.val), saved.dimLengths)
		cw.pad()
	case typeUByte:
		cw.storeUBytes(reflect.ValueOf(saved.val), saved.dimLengths)
		cw.pad()
	default:
		thrower.Throw(ErrInternal)
	}

	// then go and patch the offset
	err := cw.bf.Flush()
	thrower.ThrowIfError(err)

	// save current
	current, err := cw.file.Seek(0, os.SEEK_CUR)
	thrower.ThrowIfError(err)

	// patch
	_, err = cw.file.Seek(int64(saved.offset), os.SEEK_SET)
	thrower.ThrowIfError(err)
	write64(cw.file, offset)

	// reset to current
	_, err = cw.file.Seek(current, os.SEEK_SET)
	thrower.ThrowIfError(err)
}

func (cw *CDFWriter) Close() (err error) {
	defer thrower.RecoverError(&err)
	cw.writeAll()
	err = cw.bf.Flush()
	err2 := cw.file.Close()
	if err == nil {
		err = err2
	} else {
		// return the first error, log the second
		logger.Error(err2)
	}
	cw.file = nil
	return err
}

func writeAny(w io.Writer, any interface{}) {
	err := binary.Write(w, binary.BigEndian, any)
	thrower.ThrowIfError(err)
}

func writeBytes(w io.Writer, bytes []byte) {
	err := binary.Write(w, binary.BigEndian, bytes)
	thrower.ThrowIfError(err)
}

func write8(w io.Writer, i int8) {
	var data byte
	data = byte(i)
	err := binary.Write(w, binary.BigEndian, &data)
	thrower.ThrowIfError(err)
}

func write16(w io.Writer, i int16) {
	err := binary.Write(w, binary.BigEndian, &i)
	thrower.ThrowIfError(err)
}

func write32(w io.Writer, i int32) {
	err := binary.Write(w, binary.BigEndian, &i)
	thrower.ThrowIfError(err)
}

func write64(w io.Writer, i int64) {
	err := binary.Write(w, binary.BigEndian, &i)
	thrower.ThrowIfError(err)
}

func (cw *CDFWriter) writeName(name string) {
	// namelength
	cw.writeNumber(int64(len(name)))
	// name
	writeBytes(cw.bf, []byte(name))
	cw.pad()
}

func (cw *CDFWriter) writeNumber(n int64) {
	if cw.version < 5 {
		write32(cw.bf, int32(n))
	} else {
		write64(cw.bf, n)
	}
}

func (cw *CDFWriter) writeAll() {
	writeBytes(cw.bf, []byte("CDF"))
	write8(cw.bf, cw.version) // version 2 to handle big files
	numRecs := int64(0)       // 0: not unlimited
	cw.writeNumber(numRecs)
	if len(cw.dimLengths) > 0 {
		write32(cw.bf, fieldDimension)
		cw.writeNumber(int64(len(cw.dimLengths)))
		for dimid := int64(0); dimid < cw.nextId; dimid++ {
			name := cw.dimNames[dimid]
			cw.writeName(name)
			cw.writeNumber(cw.dimLengths[name])
		}
	} else {
		write32(cw.bf, 0)        // dimensions: absent
		cw.writeNumber(int64(0)) // dimensions: absent
	}
	cw.writeAttributes(cw.globalAttrs)
	if len(cw.vars) > 0 {
		write32(cw.bf, fieldVariable)
		cw.writeNumber(int64(len(cw.vars)))
		for i := range cw.vars {
			cw.writeVar(&cw.vars[i])
		}
		for i := range cw.vars {
			cw.writeData(cw.vars[i])
		}
	} else {
		write32(cw.bf, 0)        // variables: absent
		cw.writeNumber(int64(0)) // variables: absent
	}
}

func NewCDFWriter(fileName string) (*CDFWriter, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	bf := bufio.NewWriter(file)
	cw := &CDFWriter{
		file:        file,
		bf:          &countedWriter{bf, 0},
		vars:        nil,
		globalAttrs: nil,
		dimLengths:  make(map[string]int64),
		dimIds:      make(map[string]int64),
		dimNames:    nil,
		nextId:      0,
		version:     2}
	return cw, nil
}
