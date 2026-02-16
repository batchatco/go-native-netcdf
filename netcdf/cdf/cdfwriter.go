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

	"github.com/batchatco/go-native-netcdf/internal"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
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
	vsize      int64
}

type CDFWriter struct {
	file        *os.File
	bf          *countedWriter
	vars        []savedVar
	globalAttrs api.AttributeMap
	dimLengths  map[string]int64
	dimNames    []string
	dimIds      map[string]int64
	nextID      int64
	version     int8
	begin       int64
}

var (
	ErrUnlimitedMustBeFirst = errors.New("unlimited dimension must be first")
	ErrDimensionSize        = errors.New("dimension doesn't match size")
	ErrInvalidName          = errors.New("invalid name")
	ErrAttribute            = errors.New("invalid attribute")
	ErrEmptySlice           = errors.New("empty slice encountered")
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
		if vLen == 0 {
			// minimal support for unlimited, when it is the only dimension.
			kind = cw.scalarKind(t.Elem().Kind())
			for kind == typeNone {
				// there are other dimensions and we can't tell the size of them.
				thrower.Throw(ErrEmptySlice)
			}
			return dims, kind
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
		if !internal.IsValidNetCDFName(key) {
			return false
		}
	}
	return true
}

// AddAttributes adds attributes to be written out.
// Use util.NewOrderedMap to create attribute maps.
func (cw *CDFWriter) AddAttributes(attrs api.AttributeMap) error {
	if !hasValidNames(attrs) {
		return ErrInvalidName
	}
	cw.globalAttrs = attrs
	return nil
}

// CreateGroup is not supported in CDF.
func (cw *CDFWriter) CreateGroup(name string) (api.Writer, error) {
	return nil, api.ErrUnsupported
}

// AddVar adds a variable to be written out.
// Use util.NewOrderedMap to create attribute maps for the variable.
func (cw *CDFWriter) AddVar(name string, vr api.Variable) (err error) {
	defer thrower.RecoverError(&err)

	if !internal.IsValidNetCDFName(name) {
		return ErrInvalidName
	}
	if !hasValidNames(vr.Attributes) {
		return ErrInvalidName
	}
	// TODO: check name for validity
	cw.checkV5Attributes(vr.Attributes)
	dimLengths, ty := cw.getDimLengths(vr.Values, vr.Dimensions)
	switch ty {
	case typeUByte, typeUShort, typeUInt, typeUInt64, typeInt64:
		cw.version = 5
	}
	for i := 0; i < len(dimLengths); i++ {
		var dimName string
		if i < len(vr.Dimensions) {
			dimName = vr.Dimensions[i]
		}
		if dimName == "" {
			if ty == typeChar && i == len(dimLengths)-1 {
				dimName = fmt.Sprintf("_stringlen_%s", name)
			} else {
				dimName = fmt.Sprintf("_dimid_%d", cw.nextID)
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
			cw.dimIds[dimName] = cw.nextID
			cw.dimNames = append(cw.dimNames, dimName)
			cw.nextID++
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
		switch val := v.(type) {
		case string:
			write32(cw.bf, typeChar)
			cw.writeNumber(int64(len([]byte(val))))
			writeBytes(cw.bf, []byte(val))
			cw.pad()

		case int8:
			write32(cw.bf, typeByte)
			cw.writeNumber(1)
			writeAny(cw.bf, val)
			cw.pad()

		case []int8:
			write32(cw.bf, typeByte)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)
			cw.pad()

		case int16:
			write32(cw.bf, typeShort)
			cw.writeNumber(1)
			writeAny(cw.bf, val)
			cw.pad()

		case []int16:
			write32(cw.bf, typeShort)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)
			cw.pad()

		case int32:
			write32(cw.bf, typeInt)
			cw.writeNumber(1)
			writeAny(cw.bf, val)

		case []int32:
			write32(cw.bf, typeInt)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)

		case float32:
			write32(cw.bf, typeFloat)
			cw.writeNumber(1)
			writeAny(cw.bf, val)

		case []float32:
			write32(cw.bf, typeFloat)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)

		case float64:
			write32(cw.bf, typeDouble)
			cw.writeNumber(1)
			writeAny(cw.bf, val)

		case []float64:
			write32(cw.bf, typeDouble)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)

			// v5
		case uint8: // []uint8 and []byte are the same thing
			write32(cw.bf, typeUByte)
			cw.writeNumber(1)
			writeAny(cw.bf, val)
			cw.pad()

		case []uint8: // []uint8 and []byte are the same thing
			write32(cw.bf, typeUByte)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)
			cw.pad()

		case uint16:
			write32(cw.bf, typeUShort)
			cw.writeNumber(1)
			writeAny(cw.bf, val)
			cw.pad()

		case []uint16:
			write32(cw.bf, typeUShort)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)
			cw.pad()

		case uint32:
			write32(cw.bf, typeUInt)
			cw.writeNumber(1)
			writeAny(cw.bf, val)

		case []uint32:
			write32(cw.bf, typeUInt)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)

		case int64:
			write32(cw.bf, typeInt64)
			cw.writeNumber(1)
			writeAny(cw.bf, val)

		case []int64:
			write32(cw.bf, typeInt64)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)

		case uint64:
			write32(cw.bf, typeUInt64)
			cw.writeNumber(1)
			writeAny(cw.bf, val)

		case []uint64:
			write32(cw.bf, typeUInt64)
			cw.writeNumber(int64(len(val)))
			writeAny(cw.bf, val)

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
		case string, int8, int16, int32, float32, float64,
			[]int8, []int16, []int32, []float32, []float64:

		case []uint64, uint64, []int64, int64, []uint8, uint8, []uint16, uint16, []uint32, uint32:
			cw.version = 5
			return

		default:
			logger.Errorf("invalid attribute %#v", v)
			thrower.Throw(ErrAttribute)
		}
	}
}

func (cw *CDFWriter) writeVar(which int) {
	saved := &cw.vars[which]
	cw.writeName(saved.name)
	cw.writeNumber(int64(len(saved.dimLengths)))
	for i := range saved.dimLengths {
		name := saved.dimNames[i]
		dimID := cw.dimIds[name]
		cw.writeNumber(int64(dimID))
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
	saved.vsize = vsize

	calcOffset := cw.begin
	for i := 0; i < which; i++ {
		calcOffset += cw.vars[i].vsize
	}
	write64(cw.bf, calcOffset)
}

func (cw *CDFWriter) computeAttributeSize(attrs api.AttributeMap) int64 {
	count := int64(0)
	addLength := func() {
		count += 4
		if cw.version == 5 {
			count += 4
		}
	}
	pad := func() {
		count = (count + 3) & ^0x3
	}
	count += 4
	addLength()
	if attrs == nil || len(attrs.Keys()) == 0 {
		return count
	}
	for _, k := range attrs.Keys() {
		v, _ := attrs.Get(k)
		// name
		addLength()
		count += int64(len(k))
		pad()

		// type
		count += 4

		// nelems
		addLength()
		switch val := v.(type) {
		case string:
			count += int64(len(val))

		case int8:
			count += 1

		case []int8:
			count += int64(len(val))

		case int16:
			count += 2

		case []int16:
			count += 2 * int64(len(val))

		case int32:
			count += 4

		case []int32:
			count += 4 * int64(len(val))

		case float32:
			count += 4

		case []float32:
			count += 4 * int64(len(val))

		case float64:
			count += 8

		case []float64:
			count += 8 * int64(len(val))

			// v5
		case uint8: // []uint8 and []byte are the same thing
			count += 1

		case []uint8: // []uint8 and []byte are the same thing
			count += int64(len(val))

		case uint16:
			count += 2

		case []uint16:
			count += 2 * int64(len(val))

		case uint32:
			count += 4

		case []uint32:
			count += 4 * int64(len(val))

		case int64:
			count += 8

		case []int64:
			count += 8 * int64(len(val))

		case uint64:
			count += 8

		case []uint64:
			count += 8 * int64(len(val))

		default:
			logger.Warnf("Unknown type %T, %#v=%#v", v, k, v)
			thrower.Throw(ErrUnknownType)
		}
		pad()
	}
	return count
}

func (cw *CDFWriter) computeVarSize(saved *savedVar) int64 {
	count := int64(0)
	addLength := func() {
		count += 4
		if cw.version == 5 {
			count += 4
		}
	}
	pad := func() {
		count = (count + 3) & ^0x3
	}
	// name
	addLength()
	count += int64(len(saved.name))
	pad()

	// dims
	addLength()
	// dims
	count += int64(len(saved.dimLengths) * 4)
	if cw.version == 5 {
		count += int64(len(saved.dimLengths) * 4)
	}

	// attributes
	count += cw.computeAttributeSize(saved.attrs)
	// type
	count += 4
	// vsize
	addLength()
	// offset
	count += 8
	return count
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
}

// Close writes all the data out and closes the file.
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
	data := byte(i)
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
		for dimid := int64(0); dimid < cw.nextID; dimid++ {
			name := cw.dimNames[dimid]
			cw.writeName(name)
			cw.writeNumber(cw.dimLengths[name])
		}
	} else {
		write32(cw.bf, 0)        // dimensions: absent
		cw.writeNumber(int64(0)) // dimensions: absent
	}
	cw.checkV5Attributes(cw.globalAttrs)
	cw.writeAttributes(cw.globalAttrs)
	if len(cw.vars) > 0 {
		write32(cw.bf, fieldVariable)
		cw.writeNumber(int64(len(cw.vars)))

		// Calculate the beginning of where the data is going to be stored.
		// The var entries will need that to calculate their offset.
		cw.begin = cw.bf.Count()
		for i := range cw.vars {
			cw.begin += cw.computeVarSize(&cw.vars[i])
		}

		for i := range cw.vars {
			cw.writeVar(i)
		}
		for i := range cw.vars {
			cw.writeData(cw.vars[i])
		}
	} else {
		write32(cw.bf, 0)        // variables: absent
		cw.writeNumber(int64(0)) // variables: absent
	}
}

// OpenWriter creates the file and make it available for writing
// using AddVar and AddGlobalAttrs.  The file must be closed to actually
// write it out.
func OpenWriter(fileName string) (api.Writer, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	bf := bufio.NewWriter(file)
	globalAttrs, err := util.NewOrderedMap(
		[]string{ncpKey},
		map[string]interface{}{
			ncpKey: "version=2,github.com/batchatco/go-native-netcdf=1.0",
		})
	thrower.ThrowIfError(err)
	cw := &CDFWriter{
		file:        file,
		bf:          &countedWriter{bf, 0},
		vars:        nil,
		globalAttrs: globalAttrs,
		dimLengths:  make(map[string]int64),
		dimIds:      make(map[string]int64),
		dimNames:    nil,
		nextID:      0,
		version:     2}
	return cw, nil
}
