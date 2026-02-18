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
	name       string
	baseType   string
	baseGoType string
	val        api.Variable
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
		{"str", "string", "string",
			api.Variable{
				Values:     "a",
				Dimensions: nil,
				Attributes: nil}},
		{"strx1", "string", "string",
			api.Variable{
				Values:     "a",
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"strx2", "string", "string",
			api.Variable{
				Values:     []string{"ab", "cd"},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"f32", "float", "float32",
			api.Variable{
				Values:     float32(-10.1),
				Dimensions: nil,
				Attributes: nil}},
		{"f32x1", "float", "float32",
			api.Variable{
				Values:     []float32{-10.1},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"f32x2", "float", "float32",
			api.Variable{
				Values: [][]float32{{-10.1, 10.1},
					{-20.2, 20.2}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"f64", "double", "float64",
			api.Variable{
				Values:     float64(-10.1),
				Dimensions: nil,
				Attributes: nil}},
		{"f64x1", "double", "float64",
			api.Variable{
				Values:     []float64{-10.1},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"f64x2", "double", "float64",
			api.Variable{
				Values: [][]float64{[]float64{-10.1, 10.1},
					{-20.2, 20.2}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"i8", "byte", "int8",
			api.Variable{
				Values:     int8(-10),
				Dimensions: nil,
				Attributes: nil}},
		{"i8x1", "byte", "int8",
			api.Variable{
				Values:     []int8{-10},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"i8x2", "byte", "int8",
			api.Variable{
				Values: [][]int8{{-10, 10},
					{-20, 20}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"ui8", "ubyte", "uint8",
			api.Variable{
				Values:     uint8(10),
				Dimensions: nil,
				Attributes: nil}},
		{"ui8x1", "ubyte", "uint8",
			api.Variable{
				Values:     []uint8{10},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"ui8x2", "ubyte", "uint8",
			api.Variable{
				Values: [][]uint8{{10, 20},
					{20, 30}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"i16", "short", "int16",
			api.Variable{
				Values:     int16(-10000),
				Dimensions: nil,
				Attributes: nil}},
		{"i16x1", "short", "int16",
			api.Variable{
				Values:     []int16{-10000},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"i16x2", "short", "int16",
			api.Variable{
				Values: [][]int16{{-10000, 10000},
					{-20000, 20000}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"ui16", "ushort", "uint16",
			api.Variable{
				Values:     uint16(10000),
				Dimensions: nil,
				Attributes: nil}},
		{"ui16x1", "ushort", "uint16",
			api.Variable{
				Values:     []uint16{10000},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"ui16x2", "ushort", "uint16",
			api.Variable{
				Values: [][]uint16{{10000, 20000},
					{20000, 30000}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"i32", "int", "int32",
			api.Variable{
				Values:     int32(-10000000),
				Dimensions: nil,
				Attributes: nil}},
		{"i32x1", "int", "int32",
			api.Variable{
				Values:     []int32{-10000000},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"i32x2", "int", "int32",
			api.Variable{
				Values: [][]int32{{-10000000, 10000000},
					{-20000000, 20000000}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"ui32", "uint", "uint32",
			api.Variable{
				Values:     uint32(10000000),
				Dimensions: nil,
				Attributes: nil}},

		{"ui32x1", "uint", "uint32",
			api.Variable{
				Values:     []uint32{10000000},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"ui32x2", "uint", "uint32",
			api.Variable{
				Values: [][]uint32{{10000000, 20000000},
					{20000000, 30000000}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"i64", "int64", "int64",
			api.Variable{
				Values:     int64(-10000000000),
				Dimensions: nil,
				Attributes: nil}},
		{"i64x1", "int64", "int64",
			api.Variable{
				Values:     []int64{-10000000000},
				Dimensions: []string{"dim"},
				Attributes: nil}},
		{"i64x2", "int64", "int64",
			api.Variable{
				Values: [][]int64{{-10000000000, 10000000000},
					{-20000000000, 20000000000}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
		{"ui64", "uint64", "uint64",
			api.Variable{
				Values:     uint64(10000000000),
				Dimensions: nil,
				Attributes: nil}},
		{"ui64x1", "uint64", "uint64",
			api.Variable{
				Values:     []uint64{10000000000},
				Dimensions: []string{"dim"},
				Attributes: nil}},

		{"ui64x2", "uint64", "uint64",
			api.Variable{
				Values: [][]uint64{{10000000000, 20000000000},
					{20000000000, 30000000000}},
				Dimensions: []string{"d1", "d2"},
				Attributes: nil}},
	}

	fills = keyValList{
		{"strx1", "string", "string", api.Variable{
			Values:     "aa",
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"f32x1", "float", "float32", api.Variable{
			Values:     []float32{10.1, 10.1},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"f64x1", "double", "float64", api.Variable{
			Values:     []float64{10.1, 10.1},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i8x1", "byte", "int8", api.Variable{
			Values:     []int8{10, 10},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui8x1", "ubyte", "uint8", api.Variable{
			Values:     []uint8{20, 20},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i16x1", "short", "int16", api.Variable{
			Values:     []int16{10000, 10000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui16x1", "ushort", "uint16", api.Variable{
			Values:     []uint16{20000, 20000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i32x1", "int", "int32", api.Variable{
			Values:     []int32{10000000, 10000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui32x1", "uint", "uint32", api.Variable{
			Values:     []uint32{20000000, 20000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"i64x1", "int64", "int64", api.Variable{
			Values:     []int64{10000000000, 10000000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
		{"ui64x1", "uint64", "uint64", api.Variable{
			Values:     []uint64{20000000000, 20000000000},
			Dimensions: []string{"d1"},
			Attributes: nil}},
	}
)

func (kl keyValList) check(t *testing.T, name string, val api.VarGetter, values any) bool {
	t.Helper()
	for _, kv := range kl {
		if name != kv.name {
			continue
		}
		if values == nil {
			var err error
			values, err = val.Values()
			if err != nil {
				t.Error(err)
				return false
			}
		}
		if !reflect.DeepEqual(values, kv.val.Values) {
			t.Logf("var deepequal %s got type=%T, exp type=%T, got val=%v, exp val=%v", name, values, kv.val.Values,
				values, kv.val.Values)
			return false
		}
		attrs := val.Attributes()
		if !reflect.DeepEqual(attrs.Keys(), kv.val.Attributes.Keys()) &&
			!(len(attrs.Keys()) == 0 && len(kv.val.Attributes.Keys()) == 0) {
			t.Log("attr")
			return false
		}
		for _, v := range attrs.Keys() {
			a, has := attrs.Get(v)
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
		dims := val.Dimensions()
		if !reflect.DeepEqual(dims, kv.val.Dimensions) &&
			!(len(dims) == 0 && len(kv.val.Dimensions) == 0) {
			t.Log("dims", dims, kv.val.Dimensions)
			return false
		}
		typ := val.Type()
		if typ != kv.baseType {
			t.Log("type mismatch got=", typ, "exp=", kv.baseType)
		}
		typ = val.GoType()
		if typ != kv.baseGoType {
			t.Log("go type mismatch got=", typ, "exp=", kv.baseGoType)
		}
		return true
	}
	t.Log("none")
	return false
}

func ndims(t *testing.T, val any) int {
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

// closeCW closes the CDF writer and checks for errors.
// It sets the pointer to nil when it is completed, and
// so can be called twice, which may happen when it a
// close is deferred, but also need to be done inline.
func closeCW(t *testing.T, cw *api.Writer, fileName string) {
	t.Helper()
	if *cw == nil {
		return
	}
	err := (*cw).Close()
	if err != nil {
		t.Error(err)
	}
	*cw = nil
	// Ensure we wrote a file that ncdump can read
	if !ncDump(t, fileName) {
		t.Error("ncdump could not open", fileName)
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	for i, v := range values {
		var om *util.OrderedMap
		var err error
		if ndims(t, v.val.Values) >= 2 {
			om, err = util.NewOrderedMap(nil, nil)
		} else {
			om, err = util.NewOrderedMap([]string{"attr"},
				map[string]any{"attr": v.val.Values})
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
	closeCW(t, &cw, fileName) // writes out the data, can't be deferred
	nc, err := Open(fileName)
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
		t.Error("mismatch", "got=", len(vars), "exp=", len(values))
		return
	}
	for _, name := range vars {
		vg, err := nc.GetVarGetter(name)
		if err != nil {
			t.Error(err)
			return
		}
		vr, err := nc.GetVariable(name)
		if err != nil {
			t.Error(err)
			return
		}
		if !values.check(t, name, vg, vr.Values) {
			t.Error("check")
			return
		}
		if !values.check(t, name, vg, nil) {
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

func commonOneDim(t *testing.T, slow convertType) {
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
	defer closeCW(t, &cw, genName) // okay to call this twice, sets cw to nil
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
	closeCW(t, &cw, genName) // writes out the data, can't be deferred
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

func TestMultiDim(t *testing.T) {
	// Set up
	fileName := "testdata/testmultidim" // base filename without extension
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
	defer nc.Close()

	vrVal, err := nc.GetVariable("val")
	if err != nil {
		t.Error("nc.GetVariable('val') err=", err)
		return
	}
	if vrVal == (nil) {
		t.Errorf("variable val not found")
		return
	}
	val, hasVal := vrVal.Values.([][][][]uint16)
	if !hasVal {
		t.Error("variable val conversion to [][][][]uint16 failed")
		return
	}
	expected := [][][][]uint16{
		{
			{
				{0, 1, 2, 3},
				{4, 5, 6, 7},
				{8, 9, 10, 11},
			}, {
				{12, 13, 14, 15},
				{16, 17, 18, 19},
				{20, 21, 22, 23},
			},
		}, {
			{
				{100, 101, 102, 103},
				{104, 105, 106, 107},
				{108, 109, 110, 111},
			}, {
				{112, 113, 114, 115},
				{116, 117, 118, 119},
				{120, 121, 122, 123},
			},
		},
	}
	if !reflect.DeepEqual(val, expected) {
		t.Error("val did not match expected")
	}
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
		{"i8x1", "byte", "int8", api.Variable{
			Values:     [][]int8{{12}, {56}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nilMap}},
		{"i16x1", "short", "int16", api.Variable{
			Values:     []int16{9876, 5432},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
		{"i32x1", "int", "int32", api.Variable{
			Values:     []int32{12, 34},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
		// ncgen doesn't generate int64 properly for cdf v5, using double to get 8-byte
		// scalars instead.
		{"dx1", "double", "float64", api.Variable{
			Values:     []float64{5.6e100, 7.8e100},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, unlims)
}

func commonUnlimitedEmpty(t *testing.T, slow convertType) {
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
		{"a", "int", "int32", api.Variable{
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
		switch version {
		case "classic", "64-bit offset":
			fileName = "testdata/testunlimited"
		case "64-bit data":
			fileName = "testdata/testcdf5"
		}
		genName := ncGenVersion(t, version, fileName)
		if genName == "" {
			t.Error(errorNcGen, version)
			continue
		}
		nc, err := Open(genName)
		if err != nil {
			t.Error(err, version)
			continue
		}
		_ = nc.ListVariables()
		attrs := nc.Attributes()
		hidden, has := attrs.Get(ncpKey)
		if has {
			t.Log(version, "hidden property=", hidden)
		} else {
			t.Log("Hidden property not found", version)
		}
		nc.Close()
		os.Remove(genName)
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
		{"i8x1", "byte", "int8", api.Variable{
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
		{"i16x1", "short", "int16", api.Variable{
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	for i, v := range fills {
		val := reflect.ValueOf(v.val.Values)
		om, err := util.NewOrderedMap(
			[]string{"TestFill"},
			map[string]any{
				"TestFill": val.Index(0).Interface()})
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
	closeCW(t, &cw, fileName) // writes out the data, can't be deferred
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	attributes, err := util.NewOrderedMap([]string{"gattr"},
		map[string]any{"gattr": []float64{2.71828, 3.14159}})
	if err != nil {
		t.Error(err)
		return
	}
	err = cw.AddAttributes(attributes)
	if err != nil {
		t.Error(err)
		return
	}
	closeCW(t, &cw, fileName) // writes out the data, can't be deferred
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
	typ, has := amap.GetType("gattr")
	if !has {
		t.Error("type missing")
		return
	}
	if typ != "double(2)" {
		t.Error("type mismatch got=", typ, "exp=", "double(2)")
		return
	}
	goType, has := amap.GetGoType("gattr")
	if !has {
		t.Error("type missing")
		return
	}
	if goType != "[]float64" {
		t.Error("type mismatch got=", goType, "exp=", "[]float64")
		return
	}
	ar, has := attr.([]float64)
	if !has {
		t.Errorf("Wrong type for attr %T", ar)
	}
	if len(ar) != 2 {
		t.Error("Wrong length for array", len(ar))
	}
	if ar[0] != 2.71828 || ar[1] != 3.14159 {
		t.Error("wrong values for attr", ar)
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	for _, v := range fills {
		err = cw.AddVar(v.name, v.val)
		if err != nil {
			t.Error(err)
			return
		}
	}
	closeCW(t, &cw, fileName) // writes out the data, can't be deferred
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
	//defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
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
	closeCW(t, &cw, fileName)
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	err = cw.AddVar("astring", api.Variable{
		Values:     []string{"short", "abcdefg"},
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	closeCW(t, &cw, fileName) // writes out the data, can't be deferred
	nc, err := Open(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("astring")
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
	if vr.Dimensions[0] != "_dimid_0" || vr.Dimensions[1] != "_stringlen_astring" {
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	err = cw.AddVar("ivec", api.Variable{
		Values:     []int32{0, 1, 2},
		Dimensions: nil,
		Attributes: nil})
	if err != nil {
		t.Error(err)
		return
	}
	closeCW(t, &cw, fileName) // writes out the data, can't be deferred
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
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil
	attributes, err := util.NewOrderedMap([]string{"not valid "},
		map[string]any{"not valid ": 3.14})
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
	err = cw.AddAttributes(attributes)
	if err != ErrInvalidName {
		t.Error(err)
		return
	}
}

func TestReservedName(t *testing.T) {
	fileName := "testdata/testreservedname.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer closeCW(t, &cw, fileName) // okay to call this twice, sets cw to nil

	// Names starting with underscore are reserved for system use
	invalidNames := []string{
		"_reserved",
		"_FillValue",
	}
	// Invalid variable name
	for _, name := range invalidNames {
		err = cw.AddVar(name, api.Variable{
			Values:     int32(0),
			Dimensions: nil,
			Attributes: nil})
		if err != ErrInvalidName {
			t.Errorf("Invalid variable name %q not detected: %v", name, err)
			return
		}
	}
	// Underscore-prefixed names are system-reserved attributes,
	// and they are NOT allowed from users.
	for _, name := range invalidNames {
		attrs, err := util.NewOrderedMap([]string{name},
			map[string]any{
				name: int32(0),
			})
		if err != nil {
			t.Error("Error creating attribute map", err)
			return
		}
		err = cw.AddAttributes(attrs)
		if err == nil {
			t.Errorf("System attribute name %q should be rejected", name)
			return
		}
	}

	// Type names are valid NetCDF names (only reserved in CDL syntax)
	typeNames := []string{
		"byte", "char", "string", "short", "int", "float", "double",
	}
	typeVals := []any{
		int8(0), "c", "s", int16(0), int32(0), float32(0), float64(0),
	}
	for i, name := range typeNames {
		err = cw.AddVar(name, api.Variable{
			Values:     typeVals[i],
			Dimensions: nil,
			Attributes: nil})
		if err != nil {
			t.Errorf("Type name %q should be valid as variable name: %v", name, err)
			return
		}
	}
}

func TestDimensions(t *testing.T) {
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
	dims := nc.ListDimensions()
	if len(dims) != 2 || dims[0] != "d1" || dims[1] != "d2" {
		t.Error("Dimensions are wrong", dims)
		return
	}
	d1, has1 := nc.GetDimension("d1")
	d2, has2 := nc.GetDimension("d2")
	if !has1 || !has2 {
		t.Error("Missing dimensions")
	}
	if d1 != 0 || d2 != 1 {
		t.Error("Dimension values are wrong", d1, d2)
	}
}
