package hdf5

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/batchatco/go-thrower"
)

type vlenManagerType struct{}

var (
	vlenManager             = vlenManagerType{}
	_           typeManager = vlenManager
)

func (vlenManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	if attr.vtType == 1 {
		// It's a string
		return "string"
	}
	vAttr := attr.children[0]
	ty := cdlTypeString(vAttr.class, sh, name, vAttr, origNames)
	assert(ty != "", "unable to parse vlen attr")
	signature := fmt.Sprintf("%s(*)", ty)
	namedType := sh.findSignature(signature, name, origNames, cdlTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (vlenManagerType) goTypeString(sh sigHelper, typeName string, attr *attribute, origNames map[string]bool) string {
	if attr.vtType == 1 {
		// It's a string
		return "string"
	}
	vAttr := attr.children[0]
	ty := goTypeString(vAttr.class, sh, typeName, vAttr, origNames)
	assert(ty != "", "unable to parse vlen attr")
	signature := fmt.Sprintf("[]%s", ty)
	namedType := sh.findSignature(signature, typeName, origNames, goTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (vlenManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("* variable-length, dtlength=", attr.length,
		"proplen=", bf.Rem())
	// checkVal(1, dtversion, "Only support version 1 of variable-length")
	vtType := uint8(bitFields & 0b1111) // XXX: we will need other bits too for decoding
	vtPad := uint8(bitFields>>4) & 0b1111
	// The value of pad here may not have anything to do with reading data, just
	// writing.  So we could accept all of them
	assert(vtPad == 0 || vtPad == 1, "only do v0 and v1 versions of VL padding")
	vtCset := (bitFields >> 8) & 0b1111
	logger.Infof("type=%d paddingtype=%d cset=%d", vtType, vtPad, vtCset)
	switch vtType {
	case 0:
		checkVal(0, vtCset, "cset when not string")
		logger.Infof("sequence")
	case 1:
		if vtCset == 0 {
			logger.Infof("string (ascii)")
		} else {
			logger.Infof("string (utf8)")
		}
	default:
		fail("unknown variable-length type")
	}
	var variableAttr attribute
	printDatatype(hr, c, bf, nil, 0, &variableAttr)
	logger.Info("variable class", variableAttr.class, "vtType", vtType)
	attr.children = append(attr.children, &variableAttr)
	attr.vtType = vtType
	rem := int64(0)
	if df != nil {
		rem = df.Rem()
	}
	if rem < int64(attr.length) {
		logger.Infof("variable-length short data: %d vs. %d", rem, attr.length)
		return
	}
	logger.Info("len data is", rem, "dlen", df.Count())

	attr.df = newResetReaderSave(df, df.Rem())
	logger.Infof("Type of this vattr: %T", attr.value)
}

func (vlenManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return []byte{0}
}

func (vlenManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	logger.Info("dimensions=", dimensions)
	if attr.vtType == 1 {
		// It's a string
		// TODO: use the padding and character set information
		logger.Info("variable-length string", len(dimensions))
		return allocStrings(hr, bf, dimensions) // already converted
	}
	logger.Info("variable-length type", typeNames[int(attr.children[0].class)])
	logger.Info("dimensions=", dimensions, "rem=", bf.(remReader).Rem())
	cast := c.cast(*attr)
	values := allocVariable(hr, c, bf, dimensions, *attr.children[0], cast)
	logger.Infof("vl kind %T", values)
	if cast != nil {
		return values
	}
	return convert(values)
}

func allocStrings(hr heapReader, bf io.Reader, dimLengths []uint64) interface{} {
	logger.Info("allocStrings", dimLengths)
	if len(dimLengths) == 0 {
		// alloc one scalar
		var length uint32
		var addr uint64
		var index uint32

		var err error
		err = binary.Read(bf, binary.LittleEndian, &length)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &addr)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &index)
		thrower.ThrowIfError(err)
		logger.Infof("String length %d (0x%x), addr 0x%x, index %d (0x%x)",
			length, length, addr, index, index)
		if length == 0 {
			return ""
		}
		bff, sz := hr.readGlobalHeap(addr, index)
		s := make([]byte, sz)
		read(bff, s)
		logger.Info("string=", string(s))
		return getString(s) // TODO: should be s[:length]
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]string, thisDim)
		for i := uint64(0); i < thisDim; i++ {
			var length uint32
			var addr uint64
			var index uint32

			err := binary.Read(bf, binary.LittleEndian, &length)
			thrower.ThrowIfError(err)
			err = binary.Read(bf, binary.LittleEndian, &addr)
			thrower.ThrowIfError(err)
			err = binary.Read(bf, binary.LittleEndian, &index)
			thrower.ThrowIfError(err)
			logger.Infof("String length %d (0x%x), addr 0x%x, index %d (0x%x)",
				length, length, addr, index, index)
			if length == 0 {
				values[i] = ""
				continue
			}
			bff, sz := hr.readGlobalHeap(addr, index)
			s := make([]byte, sz)
			read(bff, s)
			values[i] = getString(s) // TODO: should be s[:length]
		}
		return values
	}
	ty := reflect.TypeOf("")
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocStrings(hr, bf, dimLengths[1:])))
	}
	return vals.Interface()
}

func allocVariable(hr heapReader, c caster, bf io.Reader, dimLengths []uint64, attr attribute,
	cast reflect.Type) interface{} {
	logger.Info("allocVariable", dimLengths, "count=", bf.(remReader).Count(),
		"rem=", bf.(remReader).Rem())
	if len(dimLengths) == 0 {
		var length uint32
		var addr uint64
		var index uint32
		err := binary.Read(bf, binary.LittleEndian, &length)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &addr)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &index)
		thrower.ThrowIfError(err)
		logger.Infof("length %d(0x%x) addr 0x%x index %d(0x%x)\n",
			length, length, addr, index, index)
		var val0 interface{}
		var s []byte
		var bff remReader
		if length == 0 {
			// If there's no value to read, we fake one to get the type.
			attr.dimensions = nil
			s = make([]byte, attr.length)
			bff = newResetReaderFromBytes(s)
		} else {
			bff, _ = hr.readGlobalHeap(addr, index)
		}
		var t reflect.Type
		val0 = getDataAttr(hr, c, bff, attr)
		if cast != nil {
			t = cast.Elem()
		} else {
			t = reflect.ValueOf(val0).Type()
		}
		sl := reflect.MakeSlice(reflect.SliceOf(t), int(length), int(length))
		if cast != nil {
			sl = sl.Convert(cast)
		}
		if length > 0 {
			sl.Index(0).Set(reflect.ValueOf(val0))
			for i := 1; i < int(length); i++ {
				val := getDataAttr(hr, c, bff, attr)
				sl.Index(i).Set(reflect.ValueOf(val))
			}
		}
		return sl.Interface()
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		// For scalars, this can be faster using binary.Read
		vals := make([]interface{}, thisDim)
		for i := uint64(0); i < thisDim; i++ {
			logger.Info("Alloc inner", i, "of", thisDim)
			vals[i] = allocVariable(hr, c, bf, dimLengths[1:], attr, cast)
		}
		assert(vals[0] != nil, "we never return nil")
		t := reflect.ValueOf(vals[0]).Type()
		vals2 := reflect.MakeSlice(reflect.SliceOf(t), int(thisDim), int(thisDim))
		for i := 0; i < int(thisDim); i++ {
			vals2.Index(i).Set(reflect.ValueOf(vals[i]))
		}
		return vals2.Interface()
	}

	// TODO: we sometimes know the type (float32) and can do something smarter here

	vals := make([]interface{}, thisDim)
	for i := uint64(0); i < thisDim; i++ {
		logger.Info("Alloc outer", i, "of", thisDim)
		vals[i] = allocVariable(hr, c, bf, dimLengths[1:], attr, cast)
	}
	t := reflect.ValueOf(vals[0]).Type()
	vals2 := reflect.MakeSlice(reflect.SliceOf(t), int(thisDim), int(thisDim))
	for i := 0; i < int(thisDim); i++ {
		vals2.Index(i).Set(reflect.ValueOf(vals[i]))
	}
	return vals2.Interface()
}
