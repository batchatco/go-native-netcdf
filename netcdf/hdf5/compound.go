package hdf5

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/batchatco/go-thrower"
)

type compoundManagerType struct{}

type compoundField struct {
	Name string
	Val  interface{}
}
type compound []compoundField

var (
	compoundManager             = compoundManagerType{}
	_               typeManager = compoundManager
)

func (compoundManagerType) cdlTypeString(sh sigHelper, name string, attr *attribute, origNames map[string]bool) string {
	members := make([]string, len(attr.children))
	for i, cattr := range attr.children {
		ty := cdlTypeString(cattr.class, sh, name, cattr, origNames)
		members[i] = fmt.Sprintf("\t%s %s;\n", ty, cattr.name)
	}
	interior := strings.Join(members, "")
	signature := fmt.Sprintf("compound {\n%s}", interior)
	namedType := sh.findSignature(signature, name, origNames, cdlTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (compoundManagerType) goTypeString(sh sigHelper, typeName string, attr *attribute, origNames map[string]bool) string {
	members := make([]string, len(attr.children))
	for i, cattr := range attr.children {
		ty := goTypeString(cattr.class, sh, typeName, cattr, origNames)
		members[i] = fmt.Sprintf("\t%s %s", cattr.name, ty)
	}
	interior := strings.Join(members, "\n")
	signature := fmt.Sprintf("struct {\n%s\n}\n", interior)
	namedType := sh.findSignature(signature, typeName, origNames, goTypeString)
	if namedType != "" {
		return namedType
	}
	return signature
}

func (compoundManagerType) alloc(hr heapReader, c caster, bf io.Reader, attr *attribute,
	dimensions []uint64) interface{} {
	cast := c.cast(*attr)
	values := allocCompounds(hr, c, bf, dimensions, *attr, cast)
	return values
}

func (compoundManagerType) defaultFillValue(obj *object, objFillValue []byte, undefinedFillValue bool) []byte {
	return objFillValue
}

func (compoundManagerType) parse(hr heapReader, c caster, attr *attribute, bitFields uint32, bf remReader, df remReader) {
	logger.Info("* compound")
	logger.Info("attr.dtversion", attr.dtversion)
	assert(attr.dtversion >= 1 && attr.dtversion <= maxDTVersion,
		fmt.Sprintln("compound datatype version", attr.dtversion, "not supported"))
	nmembers := bitFields & 0b11111111
	logger.Info("* number of members:", nmembers)

	padding := 7
	switch attr.dtversion {
	case dtversionStandard, dtversionArray:
		break
	default:
		padding = 0
	}
	rem := int64(0)
	if df != nil {
		rem = df.Rem()
	}
	for i := 0; i < int(nmembers); i++ {
		name := readNullTerminatedName(bf, padding)
		logger.Info(i, "compound name=", name)
		var byteOffset uint32
		var nbytes uint8
		switch attr.dtversion {
		case dtversionStandard, dtversionArray:
			nbytes = 4
		case dtversionPacked, dtversionV4:
			switch {
			case attr.length < 256:
				nbytes = 1
			case attr.length < 65536:
				nbytes = 2
			case attr.length < 16777216:
				nbytes = 3
			default:
				nbytes = 4
			}
		}
		byteOffset = uint32(readEnc(bf, nbytes))
		logger.Infof("[%d] byteOffset=0x%x", nbytes, byteOffset)
		var compoundAttribute attribute
		compoundAttribute.name = name
		compoundAttribute.byteOffset = byteOffset
		if attr.dtversion == dtversionStandard {
			dimensionality := read8(bf)
			logger.Info("dimensionality", dimensionality)
			checkZeroes(bf, 3)
			perm := read32(bf)
			logger.Info("permutation", perm)
			if perm != 0 {
				maybeFail(
					fmt.Sprint("permutation field should be zero, was ", perm))
			}
			reserved := read32(bf)
			checkVal(0, reserved, "reserved dt")
			compoundAttribute.dimensions = make([]uint64, 4)
			for i := 0; i < 4; i++ {
				dsize := read32(bf)
				logger.Info("dimension", i, "size", dsize)
				compoundAttribute.dimensions[i] = uint64(dsize)
			}
			compoundAttribute.dimensions = compoundAttribute.dimensions[:dimensionality]
		}

		logger.Infof("%d compound before: len(prop) = %d len(data) = %d", i, bf.Rem(), rem)
		printDatatype(hr, c, bf, nil, 0, &compoundAttribute)
		logger.Infof("%d compound after: len(prop) = %d len(data) = %d", i, bf.Rem(), rem)
		logger.Infof("%d compound dtlength", compoundAttribute.length)
		attr.children = append(attr.children, &compoundAttribute)
	}
	logger.Info("Compound length is", attr.length)
	if rem > 0 {
		attrSize := calcAttrSize(attr)
		logger.Info("compound alloced", df.Count(), df.Rem()+df.Count(),
			"attrSize=", attrSize)
		bff := df
		if attrSize > df.Rem() {
			logger.Info("Adding fill value reader")
			// bff = makeFillValueReader(obj, df, attrSize)
		}
		attr.df = newResetReaderSave(bff, bff.(remReader).Rem())
		logger.Info("rem=", df.Rem(), "nread=", bff.(remReader).Count())
	}
	logger.Info("Finished compound", "rem=", bf.Rem())
}

func allocCompounds(hr heapReader, cstr caster, bf io.Reader, dimLengths []uint64, attr attribute,
	cast reflect.Type) interface{} {
	length := int64(attr.length)
	class := typeNames[attr.class]
	logger.Info(bf.(remReader).Count(), "Alloc compounds", dimLengths, class,
		"length=", length,
		"nchildren=", len(attr.children), "rem=", bf.(remReader).Rem())
	dtlen := uint64(0)
	for i := range attr.children {
		clen := uint64(calcAttrSize(attr.children[i]))
		dtlen += clen
	}
	if len(dimLengths) == 0 {
		rem := bf.(remReader).Rem()
		if length > rem {
			logger.Warn("not enough room", length, rem)
		} else {
			rem = length
		}
		cbf := newResetReader(bf, rem)
		varray := make([]compoundField, len(attr.children))
		for i, c := range attr.children {
			byteOffset := c.byteOffset
			clen := uint64(calcAttrSize(c))
			if int64(byteOffset) > cbf.Count() {
				skipLen := int64(byteOffset) - cbf.Count()
				logger.Info("skip to offset", skipLen)
				skip(cbf, int64(skipLen))
			}
			ccbf := newResetReader(cbf, int64(clen))
			varray[i].Val = getDataAttr(hr, cstr, ccbf, *c)
			varray[i].Name = c.name
		}
		if cbf.Count() < length {
			rem := length - cbf.Count()
			skip(cbf, int64(rem))
		}
		if cast != nil {
			fields := make([]reflect.StructField, len(attr.children))
			for i := range fields {
				fields[i] = cast.Field(i)
			}
			stp := reflect.New(reflect.StructOf(fields))
			st := reflect.Indirect(stp)
			for i := range varray {
				assertError(st.Field(i).CanSet(), ErrNonExportedField,
					"can't set non-exported field")
				st.Field(i).Set(reflect.ValueOf(varray[i].Val))
			}
			return st.Interface()
		}
		logger.Info(bf.(remReader).Count(), "return compound count=", bf.(remReader).Count())
		return compound(varray)
	}
	var t reflect.Type
	if cast != nil {
		t = cast
	} else {
		var x compound
		t = reflect.TypeOf(x)
	}
	vals2 := makeSlices(t, dimLengths)
	thisDim := dimLengths[0]
	for i := uint64(0); i < thisDim; i++ {
		if !vals2.Index(int(i)).CanSet() {
			thrower.Throw(ErrNonExportedField)
		}
		vals2.Index(int(i)).Set(reflect.ValueOf(allocCompounds(hr, cstr, bf, dimLengths[1:], attr, cast)))
	}
	return vals2.Interface()
}
