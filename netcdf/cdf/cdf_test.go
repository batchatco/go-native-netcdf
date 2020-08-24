package cdf

// 1. Need test of invalid fill value (slice instead of scalar)
// 2. Need test of V1 file.
// 3. Need test of null in name.
// 4. Need test of too many dimensions.

import (
	"os"
	"os/exec"
	"reflect"
	"strings"
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
			Values:     "a",
			Dimensions: nil,
			Attributes: nil}},
		{"strx1", api.Variable{
			Values:     "a",
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"strx2", api.Variable{
			Values:     []string{"ab", "cd"},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"f32", api.Variable{
			Values:     float32(-10.1),
			Dimensions: nil,
			Attributes: nil}},
		{"f32x1", api.Variable{
			Values:     []float32{-10.1},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"f32x2", api.Variable{
			Values: [][]float32{{-10.1, 10.1},
				{-20.2, 20.2}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"f64", api.Variable{
			Values:     float64(-10.1),
			Dimensions: nil,
			Attributes: nil}},
		{"f64x1", api.Variable{
			Values:     []float64{-10.1},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"f64x2", api.Variable{
			Values: [][]float64{[]float64{-10.1, 10.1},
				{-20.2, 20.2}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"i8", api.Variable{
			Values:     int8(-10),
			Dimensions: nil,
			Attributes: nil}},
		{"i8x1", api.Variable{
			Values:     []int8{-10},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"i8x2", api.Variable{
			Values: [][]int8{{-10, 10},
				{-20, 20}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"ui8", api.Variable{
			Values:     uint8(10),
			Dimensions: nil,
			Attributes: nil}},
		{"ui8x1", api.Variable{
			Values:     []uint8{10},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"ui8x2", api.Variable{
			Values: [][]uint8{{10, 20},
				{20, 30}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"i16", api.Variable{
			Values:     int16(-10000),
			Dimensions: nil,
			Attributes: nil}},
		{"i16x1", api.Variable{
			Values:     []int16{-10000},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"i16x2", api.Variable{
			Values: [][]int16{{-10000, 10000},
				{-20000, 20000}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"ui16", api.Variable{
			Values:     uint16(10000),
			Dimensions: nil,
			Attributes: nil}},
		{"ui16x1", api.Variable{
			Values:     []uint16{10000},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"ui16x2", api.Variable{
			Values: [][]uint16{{10000, 20000},
				{20000, 30000}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"i32", api.Variable{
			Values:     int32(-10000000),
			Dimensions: nil,
			Attributes: nil}},
		{"i32x1", api.Variable{
			Values:     []int32{-10000000},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"i32x2", api.Variable{
			Values: [][]int32{{-10000000, 10000000},
				{-20000000, 20000000}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"ui32", api.Variable{
			Values:     uint32(10000000),
			Dimensions: nil,
			Attributes: nil}},

		{"ui32x1", api.Variable{
			Values:     []uint32{10000000},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"ui32x2", api.Variable{
			Values: [][]uint32{{10000000, 20000000},
				{20000000, 30000000}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"i64", api.Variable{
			Values:     int64(-10000000000),
			Dimensions: nil,
			Attributes: nil}},
		{"i64x1", api.Variable{
			Values:     []int64{-10000000000},
			Dimensions: []string{"dim"},
			Attributes: nil}},
		{"i64x2", api.Variable{
			Values: [][]int64{{-10000000000, 10000000000},
				{-20000000000, 20000000000}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
		{"ui64", api.Variable{
			Values:     uint64(10000000000),
			Dimensions: nil,
			Attributes: nil}},
		{"ui64x1", api.Variable{
			Values:     []uint64{10000000000},
			Dimensions: []string{"dim"},
			Attributes: nil}},

		{"ui64x2", api.Variable{
			Values: [][]uint64{{10000000000, 20000000000},
				{20000000000, 30000000000}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nil}},
	}

	fills = keyValList{
		{"strx1", api.Variable{
			Values:     "aa",
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"f32x1", api.Variable{
			Values:     []float32{10.1, 10.1},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"f64x1", api.Variable{
			Values:     []float64{10.1, 10.1},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i8x1", api.Variable{
			Values:     []int8{10, 10},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui8x1", api.Variable{
			Values:     []uint8{20, 20},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i16x1", api.Variable{
			Values:     []int16{10000, 10000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui16x1", api.Variable{
			Values:     []uint16{20000, 20000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i32x1", api.Variable{
			Values:     []int32{10000000, 10000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui32x1", api.Variable{
			Values:     []uint32{20000000, 20000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i64x1", api.Variable{
			Values:     []int64{10000000000, 10000000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui64x1", api.Variable{
			Values:     []uint64{20000000000, 20000000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
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

func closeCW(t *testing.T, cw **CDFWriter) {
	t.Helper()
	if *cw == nil {
		return
	}
	err := (*cw).Close()
	if err != nil {
		t.Error(err)
	}
}

func TestTypes(t *testing.T) {
	fileName := "testdata/testdata.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
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
	cw.Close()
	cw = nil
	nc, err := Open(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkAll(t, nc, values)
}

func TestNCProperties(t *testing.T) {
	fileName := "testdata/testncproperties.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	closeCW(t, &cw)
	nc, err := Open(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	attrs := nc.Attributes()
	if len(attrs.Keys()) != 0 {
		t.Error("These attributes should not have been returned:", attrs.Keys())
	}
	hidden, has := attrs.Get(ncpKey)
	if !has {
		t.Error("Hidden property", ncpKey, "not found")
		return
	}
	spl := strings.Split(hidden.(string), ",")
	if len(spl) != 2 || spl[0] != "version=2" {
		t.Error("Hidden property is not correct value:", hidden)
		return
	}
	if !strings.HasPrefix(spl[1], "github.com/batchatco/go-native-netcdf=") {
		t.Error("Hidden property is not correct value:", hidden)
		return
	}
}

func checkAll(t *testing.T, nc api.Group, values keyValList) {
	t.Helper()
	vars := nc.ListVariables()
	if len(vars) != len(values) {
		t.Error("mismatch", "got=", len(vars), "exp=", len(values))
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

func ncGenVersion(t *testing.T, version string, fileNameNoExt string) string {
	t.Helper()
	cmd := exec.Command("ncgen", "-b", "-k", version, fileNameNoExt+".cdl",
		"-o", fileNameNoExt+".nc")
	genName := fileNameNoExt + ".nc"
	err := cmd.Run()
	if err != nil {
		t.Log(err, "ncgen", "-b", "-k", version, fileNameNoExt+".cdl")
		return ""
	}
	f, r := os.Open(genName)
	if r != nil {
		t.Log("os error", r)
		return ""
	}
	f.Close()
	return genName
}

func ncGen(t *testing.T, fileNameNoExt string) string {
	t.Helper()
	return ncGenVersion(t, "nc5", fileNameNoExt)
}

const errorNcGen = "Error running ncgen command from netcdf package"

func commonOneDim(t *testing.T, slow bool) {
	t.Helper()
	// Set up
	fileName := "testdata/testonedim" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	t.Log("TEST:", genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	nc.(*CDF).slowConvert = slow
	defer nc.Close()

	// Test reading what ncgen wrote
	vr, err := nc.GetVariable("c2")
	if err != nil {
		t.Error(err)
		return
	}
	if vr.Dimensions[0] != "d1" {
		t.Error("dimension missing")
		return
	}
	if vr.Values.(string) != "b" {
		t.Error("expected \"b\"")
	}

	vr, err = nc.GetVariable("c")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("expected zero dimensions")
		return
	}
	if vr.Values.(string) != "a" {
		t.Error("expected \"a\"")
	}

	// Next test: write the file ourselves this time
	_ = os.Remove(genName)
	cw, err := OpenWriter(genName)
	defer os.Remove(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	err = cw.AddVar("c2", api.Variable{
		Values:     "b",
		Dimensions: []string{"d1"},
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddVar("c", api.Variable{
		Values:     "a",
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	cw.Close()
	cw = nil
	nc2, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc2.Close()
	vr, err = nc2.GetVariable("c2")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 1 || vr.Dimensions[0] != "d1" {
		t.Error("dimension missing")
		return
	}
	if vr.Values.(string) != "b" {
		t.Error("expected \"b\"")
	}
	vr, err = nc2.GetVariable("c")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("expected zero dimensions")
		return
	}
	if vr.Values.(string) != "a" {
		t.Error("expected \"a\"")
	}
}

func TestOneDim(t *testing.T) {
	commonOneDim(t, fast)
}

func TestOneDimSlow(t *testing.T) {
	commonOneDim(t, slow)
}

func TestUnlimited(t *testing.T) {
	fileName := "testdata/testunlimited" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)

	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i8x1", api.Variable{
			Values:     [][]int8{{12}, {56}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nilMap}},
		{"i16x1", api.Variable{
			Values:     []int16{9876, 5432},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
		{"i32x1", api.Variable{
			Values:     []int32{12, 34},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
		// ncgen doesn't generate int64 properly for cdf v5, using double to get 8-byte
		// scalars instead.
		{"dx1", api.Variable{
			Values:     []float64{5.6e100, 7.8e100},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, unlims)
}

func commonUnlimitedEmpty(t *testing.T, slow bool) {
	t.Helper()
	fileName := "testdata/testempty" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	nc.(*CDF).slowConvert = slow
	defer nc.Close()
	empty := keyValList{
		{"a", api.Variable{
			Values:     []int32{},
			Dimensions: []string{"u"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, empty)
}

func TestUnlimitedEmpty(t *testing.T) {
	commonUnlimitedEmpty(t, fast)
}

func TestUnlimitedEmptySlow(t *testing.T) {
	commonUnlimitedEmpty(t, slow)
}

func TestVersion(t *testing.T) {
	versions := []string{"classic", "64-bit data", "64-bit offset"}
	for _, version := range versions {
		var fileName string
		if version == "classic" {
			fileName = "testdata/testunlimited"
		} else {
			// 64-bit offset should not actually compile the following file
                        // because it has CDF5 types (unsigned, int64).  But it does.
			fileName = "testdata/testcdf5"
		}
		genName := ncGenVersion(t, version, fileName)
		if genName == "" {
			t.Error(errorNcGen, version)
			continue
		}
		defer os.Remove(genName)
		nc, err := Open(genName)
		if err != nil {
			t.Error(err, version)
			continue
		}
		_ = nc.ListVariables()
		nc.Close()
	}
}

func TestBadMagic(t *testing.T) {
	versions := []string{"netCDF-4", "netCDF-4 classic model"}
	for _, version := range versions {
		fileName := "testdata/testunlimited" // base filename without extension
		genName := ncGenVersion(t, version, fileName)
		if genName == "" {
			t.Error(errorNcGen, version)
			return
		}
		defer os.Remove(genName)
		nc, err := Open(genName)
		if err == nil {
			nc.Close()
			t.Error("should not have opened", version)
			continue
		}
		if err != ErrNotCDF {
			t.Error(err, version)
			return
		}
	}
}

func TestNull(t *testing.T) {
	fileName := "testdata/testnull" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	_ = nc.ListVariables()
}

func TestUnlimitedOnlyBytes(t *testing.T) {
	fileName := "testdata/testbytesonly" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i8x1", api.Variable{
			Values:     []int8{12, 56},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, unlims)
}

func TestUnlimitedOnlyShorts(t *testing.T) {
	fileName := "testdata/testshortsonly" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i16x1", api.Variable{
			Values:     []int16{9876, 5432, 7734},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, unlims)
}

func TestFill(t *testing.T) {
	fileName := "testdata/testfill.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
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
	cw.Close()
	cw = nil
	nc, err := Open(fileName)
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
	fileName := "testdata/testgattr.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	attributes, err := util.NewOrderedMap([]string{"gattr"},
		map[string]interface{}{"gattr": 3.14})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddGlobalAttrs(attributes)
	if err != nil {
		t.Error(err)
		return
	}
	cw.Close()
	cw = nil
	nc, err := Open(fileName)
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
	fileName := "testdata/testgroup.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	for _, v := range fills {
		err = cw.AddVar(v.name, v.val)
		if err != nil {
			t.Error(err)
			return
		}
	}
	cw.Close()
	cw = nil
	nc, err := Open(fileName)
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
	fileName := "testdata/testempty.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	empty := make([]int32, 0)
	err = cw.AddVar("empty", api.Variable{
		Values:     empty,
		Dimensions: []string{"d1"},
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddVar("emptystring", api.Variable{
		Values:     "",
		Dimensions: []string{"d1"},
		Attributes: nil})
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
	fileName := "testdata/teststring.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	err = cw.AddVar("string", api.Variable{
		Values:     []string{"short", "abcdefg"},
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	cw.Close()
	cw = nil
	nc, err := Open(fileName)
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
	fileName := "testdata/testdim.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	err = cw.AddVar("ivec", api.Variable{
		Values:     []int32{0, 1, 2},
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	cw.Close()
	cw = nil
	nc, err := Open(fileName)
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

func TestInvalidName(t *testing.T) {
	fileName := "testdata/testinvalidname.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw)
	attributes, err := util.NewOrderedMap([]string{"not valid "},
		map[string]interface{}{"not valid ": 3.14})
	if err != nil {
		t.Error(err)
		return
	}
	// invalid variable name
	err = cw.AddVar("not/valid", api.Variable{
		Values:     []int32{222},
		Dimensions: []string{"d1"},
		Attributes: nil})
	if err != ErrInvalidName {
		t.Error("Invalid name not detected", err)
		return
	}
	// Invalid local attribute name
	err = cw.AddVar("valid", api.Variable{
		Values:     []int32{222},
		Dimensions: []string{"d1"},
		Attributes: attributes})
	if err != ErrInvalidName {
		t.Error("Invalid name not detected", err)
		return
	}
	// Invalid global attribute name
	err = cw.AddGlobalAttrs(attributes)
	if err != ErrInvalidName {
		t.Error(err)
		return
	}
}
