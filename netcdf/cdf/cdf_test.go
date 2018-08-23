package cdf

// 1. Need test of invalid fill value (slice instead of scalar)
// 2. Need test of V1 file.
// 3. Need test of null in name.
// 4. Need test of too many dimensions.

import (
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

type keyVal struct {
	name string
	val  api.Variable
}

type keyValList []keyVal

var nilMap *util.OrderedMap

func init() {
	var err error
	nilMap, err = util.NewOrderedMap(nil, nil)
	if err != nil {
		panic(err)
	}
}

var (
	values = keyValList{
		{"str", api.Variable{
			"a",
			nil,
			nil}},
		{"strx1", api.Variable{
			"a",
			[]string{"dim"},
			nil}},
		{"strx2", api.Variable{
			[]string{"ab", "cd"},
			[]string{"d1", "d2"},
			nil}},
		{"f32", api.Variable{
			float32(-10.1),
			nil,
			nil}},
		{"f32x1", api.Variable{
			[]float32{-10.1},
			[]string{"dim"},
			nil}},
		{"f32x2", api.Variable{
			[][]float32{[]float32{-10.1, 10.1},
				[]float32{-20.2, 20.2}},
			[]string{"d1", "d2"},
			nil}},
		{"f64", api.Variable{
			float64(-10.1),
			nil,
			nil}},
		{"f64x1", api.Variable{
			[]float64{-10.1},
			[]string{"dim"},
			nil}},
		{"f64x2", api.Variable{
			[][]float64{[]float64{-10.1, 10.1},
				[]float64{-20.2, 20.2}},
			[]string{"d1", "d2"},
			nil}},
		{"i8", api.Variable{
			int8(-10),
			nil,
			nil}},
		{"i8x1", api.Variable{
			[]int8{-10},
			[]string{"dim"},
			nil}},
		{"i8x2", api.Variable{
			[][]int8{[]int8{-10, 10},
				[]int8{-20, 20}},
			[]string{"d1", "d2"},
			nil}},
		{"ui8", api.Variable{
			uint8(10),
			nil,
			nil}},
		{"ui8x1", api.Variable{
			[]uint8{10},
			[]string{"dim"},
			nil}},
		{"ui8x2", api.Variable{
			[][]uint8{[]uint8{10, 20},
				[]uint8{20, 30}},
			[]string{"d1", "d2"},
			nil}},
		{"i16", api.Variable{
			int16(-10000),
			nil,
			nil}},
		{"i16x1", api.Variable{
			[]int16{-10000},
			[]string{"dim"},
			nil}},
		{"i16x2", api.Variable{
			[][]int16{[]int16{-10000, 10000},
				[]int16{-20000, 20000}},
			[]string{"d1", "d2"},
			nil}},
		{"ui16", api.Variable{
			uint16(10000),
			nil,
			nil}},
		{"ui16x1", api.Variable{
			[]uint16{10000},
			[]string{"dim"},
			nil}},
		{"ui16x2", api.Variable{
			[][]uint16{[]uint16{10000, 20000},
				[]uint16{20000, 30000}},
			[]string{"d1", "d2"},
			nil}},
		{"i32", api.Variable{
			int32(-10000000),
			nil,
			nil}},
		{"i32x1", api.Variable{
			[]int32{-10000000},
			[]string{"dim"},
			nil}},
		{"i32x2", api.Variable{
			[][]int32{[]int32{-10000000, 10000000},
				[]int32{-20000000, 20000000}},
			[]string{"d1", "d2"},
			nil}},
		{"ui32", api.Variable{
			uint32(10000000),
			nil,
			nil}},
		{"ui32x1", api.Variable{
			[]uint32{10000000},
			[]string{"dim"},
			nil}},
		{"ui32x2", api.Variable{
			[][]uint32{[]uint32{10000000, 20000000},
				[]uint32{20000000, 30000000}},
			[]string{"d1", "d2"},
			nil}},
		{"i64", api.Variable{
			int64(-10000000000),
			nil,
			nil}},
		{"i64x1", api.Variable{
			[]int64{-10000000000},
			[]string{"dim"},
			nil}},
		{"i64x2", api.Variable{
			[][]int64{[]int64{-10000000000, 10000000000},
				[]int64{-20000000000, 20000000000}},
			[]string{"d1", "d2"},
			nil}},
		{"ui64", api.Variable{
			uint64(10000000000),
			nil,
			nil}},
		{"ui64x1", api.Variable{
			[]uint64{10000000000},
			[]string{"dim"},
			nil}},
		{"ui64x2", api.Variable{
			[][]uint64{[]uint64{10000000000, 20000000000},
				[]uint64{20000000000, 30000000000}},
			[]string{"d1", "d2"},
			nil}},
	}

	fills = keyValList{
		{"strx1", api.Variable{
			"aa",
			[]string{"d1"},
			nil}},
		{"f32x1", api.Variable{
			[]float32{10.1, 10.1},
			[]string{"d1"},
			nil}},
		{"f64x1", api.Variable{
			[]float64{10.1, 10.1},
			[]string{"d1"},
			nil}},
		{"i8x1", api.Variable{
			[]int8{10, 10},
			[]string{"d1"},
			nil}},
		{"ui8x1", api.Variable{
			[]uint8{20, 20},
			[]string{"d1"},
			nil}},
		{"i16x1", api.Variable{
			[]int16{10000, 10000},
			[]string{"d1"},
			nil}},
		{"ui16x1", api.Variable{
			[]uint16{20000, 20000},
			[]string{"d1"},
			nil}},
		{"i32x1", api.Variable{
			[]int32{10000000, 10000000},
			[]string{"d1"},
			nil}},
		{"ui32x1", api.Variable{
			[]uint32{20000000, 20000000},
			[]string{"d1"},
			nil}},
		{"i64x1", api.Variable{
			[]int64{10000000000, 10000000000},
			[]string{"d1"},
			nil}},
		{"ui64x1", api.Variable{
			[]uint64{20000000000, 20000000000},
			[]string{"d1"},
			nil}},
	}
)

func (kl keyValList) check(t *testing.T, name string, val api.Variable) bool {
	t.Helper()
	for _, kv := range kl {
		if name != kv.name {
			continue
		}
		if !reflect.DeepEqual(val.Values, kv.val.Values) {
			t.Logf("var deepequal %s %T %T %v %v", name, val.Values, kv.val.Values,
				val.Values, kv.val.Values)
			return false
		}
		if !reflect.DeepEqual(val.Attributes.Keys(), kv.val.Attributes.Keys()) &&
			!(len(val.Attributes.Keys()) == 0 && len(kv.val.Attributes.Keys()) == 0) {
			t.Log("attr")
			return false
		}
		for _, v := range val.Attributes.Keys() {
			a, has := val.Attributes.Get(v)
			if !has {
				t.Log("get val")
				return false
			}
			b, has := kv.val.Attributes.Get(v)
			if !has {
				t.Log("get kv")
				return false
			}
			rb := reflect.ValueOf(b)
			if rb.Kind() == reflect.Slice && rb.Len() == 1 {
				// Special case: single length arrays are returned as scalars
				elem := rb.Index(0)
				b = elem.Interface()
			}
			if !reflect.DeepEqual(a, b) {
				t.Logf("attr deepequal %s %T %T", name, a, b)
				return false
			}
		}
		if !reflect.DeepEqual(val.Dimensions, kv.val.Dimensions) &&
			!(len(val.Dimensions) == 0 && len(kv.val.Dimensions) == 0) {
			t.Log("dims", val.Dimensions, kv.val.Dimensions)
			return false
		}
		return true
	}
	t.Log("none")
	return false
}

func ndims(t *testing.T, val interface{}) int {
	t.Helper()
	v := reflect.ValueOf(val)
	n := 0
	for v.Kind() == reflect.Slice {
		v = v.Index(0)
		n++
	}
	if v.Kind() == reflect.String {
		n++
	}
	return n
}

func TestTypes(t *testing.T) {
	fileName := "testdata.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	for i, v := range values {
		var om *util.OrderedMap
		var err error
		if ndims(t, v.val.Values) >= 2 {
			om, err = util.NewOrderedMap(nil, nil)
		} else {
			om, err = util.NewOrderedMap([]string{"attr"},
				map[string]interface{}{"attr": v.val.Values})
		}
		if err != nil {
			t.Error(err)
			return
		}
		values[i].val.Attributes = om
		v := values[i]
		err = cw.AddVar(v.name, v.val)
		if err != nil {
			t.Error(v.name, err)
		}
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
	nc, err := NewCDF(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkAll(t, nc, values)
}

func checkAll(t *testing.T, nc api.Group, values keyValList) {
	t.Helper()
	vars := nc.ListVariables()
	if len(vars) != len(values) {
		t.Error("mismatch")
		return
	}
	for _, name := range vars {
		vr, err := nc.GetVariable(name)
		if err != nil {
			t.Error(err)
			return
		}
		if !values.check(t, name, *vr) {
			t.Error("check")
			return
		}
	}
}

func ncGen(t *testing.T, fileNameNoExt string) string {
	t.Helper()
	cmd := exec.Command("ncgen", "-b", "-5", "testdata/"+fileNameNoExt+".cdl")
	genName := fileNameNoExt + ".nc"
	err := cmd.Run()
	if err != nil {
		t.Log(err)
		return ""
	}
	return genName
}

const errorNcGen = "Error running ncgen command from netcdf package"

func TestOneDim(t *testing.T) {
	// Set up
	fileName := "testonedim" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := NewCDF(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()

	// Test reading what ncgen wrote
	vr, err := nc.GetVariable("c")
	if err != nil {
		t.Error(err)
		return
	}
	if vr.Dimensions[0] != "d1" {
		t.Error("dimension missing")
		return
	}

	// Next test: write the file ourselves this time
	_ = os.Remove(genName)
	cw, err := NewCDFWriter(genName)
	defer os.Remove(genName)
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddVar("c", api.Variable{
		Values:     "a",
		Dimensions: []string{"d1"},
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}

	nc2, err := NewCDF(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc2.Close()
	vr, err = nc2.GetVariable("c")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 1 || vr.Dimensions[0] != "d1" {
		t.Error("dimension missing")
		return
	}
}

func TestUnlimited(t *testing.T) {
	fileName := "testunlimited" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)

	nc, err := NewCDF(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i8x1", api.Variable{
			[][]int8{[]int8{12}, []int8{56}},
			[]string{"d1", "d2"},
			nilMap}},
		{"i16x1", api.Variable{
			[]int16{9876, 5432},
			[]string{"d1"},
			nilMap}},
		{"i32x1", api.Variable{
			[]int32{12, 34},
			[]string{"d1"},
			nilMap}},
		// ncgen doesn't generate int64 properly for cdf v5, using double to get 8-byte
		// scalars instead.
		{"dx1", api.Variable{
			[]float64{5.6e100, 7.8e100},
			[]string{"d1"},
			nilMap}},
	}
	checkAll(t, nc, unlims)
}

func TestUnlimitedEmpty(t *testing.T) {
	fileName := "testempty" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := NewCDF(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	empty := keyValList{
		{"a", api.Variable{
			[]int32{},
			[]string{"u"},
			nilMap}},
	}
	checkAll(t, nc, empty)
}

func TestUnlimitedOnlyBytes(t *testing.T) {
	fileName := "testbytesonly" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := NewCDF(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i8x1", api.Variable{
			[]int8{12, 56},
			[]string{"d1"},
			nilMap}},
	}
	checkAll(t, nc, unlims)
}

func TestUnlimitedOnlyShorts(t *testing.T) {
	fileName := "testshortsonly" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := NewCDF(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i16x1", api.Variable{
			[]int16{9876, 5432, 7734},
			[]string{"d1"},
			nilMap}},
	}
	checkAll(t, nc, unlims)
}

func TestFill(t *testing.T) {
	fileName := "testfill.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	for i, v := range fills {
		val := reflect.ValueOf(v.val.Values)
		om, err := util.NewOrderedMap(
			[]string{"_FillValue"},
			map[string]interface{}{
				"_FillValue": val.Index(0).Interface()})
		if err != nil {
			t.Error(err)
			return
		}
		fills[i].val.Attributes = om
		v := fills[i]
		err = cw.AddVar(v.name, v.val)
		if err != nil {
			t.Error(err)
			return
		}
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
	nc, err := NewCDF(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	vars := nc.ListVariables()
	if len(vars) != len(fills) {
		t.Error("mismatch")
		return
	}
	checkAll(t, nc, fills)
}

func TestGlobalAttributes(t *testing.T) {
	fileName := "testgattr.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	attributes, err := util.NewOrderedMap([]string{"gattr"},
		map[string]interface{}{"gattr": 3.14})
	if err != nil {
		t.Error(err)
		return
	}
	cw.AddGlobalAttrs(attributes)
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
	nc, err := NewCDF(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	amap := nc.Attributes()
	if len(amap.Keys()) != 1 {
		t.Error("length wrong")
		return
	}
	if amap.Keys()[0] != "gattr" {
		t.Error("attribute missing")
		return
	}
	attr, has := amap.Get("gattr")
	if !has {
		t.Error("attribute missing")
		return
	}
	if attr.(float64) != 3.14 {
		t.Error("wrong value for attr")
		return
	}
}

func TestGroup(t *testing.T) {
	fileName := "testgroup.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	for _, v := range fills {
		err = cw.AddVar(v.name, v.val)
		if err != nil {
			t.Error(err)
			return
		}
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
	nc, err := NewCDF(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	groups := nc.ListSubgroups()
	if len(groups) != 0 {
		t.Error(err)
		return
	}
	nc2, err := nc.GetGroup("/")
	if err != nil {
		t.Error(err)
		return
	}
	nc2.Close()
	nc2, err = nc.GetGroup("")
	if err != nil {
		t.Error(err)
		return
	}
	nc2.Close()
	nc2, err = nc.GetGroup("Nope")
	if err == nil {
		t.Error("error expected")
		nc2.Close()
		return
	}
	if err != ErrNotFound {
		t.Error("wrong error")
		return
	}
}

func TestEmpty(t *testing.T) {
	fileName := "testempty.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	empty := make([]int32, 0)
	err = cw.AddVar("empty", api.Variable{
		Values:     empty,
		Dimensions: []string{"d1"},
		Attributes: nil})
	if err != ErrEmptySlice {
		t.Error("empty slices are not allowed", err)
		return
	}
	err = cw.AddVar("emptystring", api.Variable{
		Values:     "",
		Dimensions: []string{"d1"},
		Attributes: nil})
	if err != ErrEmptySlice {
		t.Error("empty slices are not allowed", err)
		return
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
}

func getString(s string) string {
	b := []byte(s)
	end := 0
	for i := range b {
		if b[i] == 0 {
			break
		}
		end++
	}
	return string(b[:end])
}

func TestString(t *testing.T) {
	fileName := "teststring.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddVar("string", api.Variable{
		Values:     []string{"short", "abcdefg"},
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
	nc, err := NewCDF(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("string")
	if err != nil {
		t.Error(err)
		return
	}
	strvec := vr.Values.([]string)
	if len(strvec) != 2 || getString(strvec[0]) != "short" || getString(strvec[1]) != "abcdefg" {
		t.Error("bad value", strvec)
	}
	if len(vr.Dimensions) != 2 {
		t.Error("number dimensions wrong", vr.Dimensions)
	}
	if vr.Dimensions[0] != "_dimid_0" || vr.Dimensions[1] != "_stringlen_string" {
		t.Error("wrong name for dimension", vr.Dimensions)
	}
}

func TestMakeDim(t *testing.T) {
	fileName := "testdim.nc"
	_ = os.Remove(fileName)
	cw, err := NewCDFWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddVar("ivec", api.Variable{
		Values:     []int32{0, 1, 2},
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.Close()
	if err != nil {
		t.Error(err)
		return
	}
	nc, err := NewCDF(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("ivec")
	if err != nil {
		t.Error(err)
		return
	}
	ivec := vr.Values.([]int32)
	if len(ivec) != 3 || ivec[0] != 0 || ivec[1] != 1 || ivec[2] != 2 {
		t.Error("bad value", ivec)
	}
	if len(vr.Dimensions) != 1 {
		t.Error("expected a dimension")
	}
	if vr.Dimensions[0] != "_dimid_0" {
		t.Error("wrong name for dimension", vr.Dimensions)
	}
}
