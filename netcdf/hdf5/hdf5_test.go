package hdf5

import (
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

const errorNcGen = "Error running ncgen command from netcdf package"
const errorNcFilter = "Error running h5repack command from hdf5 package"

type keyVal struct {
	name string
	val  api.Variable
}

type keyValList []keyVal

var nilMap = &util.OrderedMap{}

var values = keyValList{
	{"str", api.Variable{
		Values:     "a",
		Dimensions: nil,
		Attributes: nilMap}},
	{"strx1", api.Variable{
		Values:     []string{"a"},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"strx2", api.Variable{
		Values:     [][]string{{"ab", "cd"}, {"ef", "gh"}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"f32", api.Variable{
		Values:     float32(-10.1),
		Dimensions: nil,
		Attributes: nilMap}},
	{"f32x1", api.Variable{
		Values:     []float32{-10.1},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"f32x2", api.Variable{
		Values:     [][]float32{{-10.1, 10.1}, {-20.2, 20.2}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"f64", api.Variable{
		Values:     float64(-10.1),
		Dimensions: nil,
		Attributes: nilMap}},
	{"f64x1", api.Variable{
		Values:     []float64{-10.1},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"f64x2", api.Variable{
		Values:     [][]float64{{-10.1, 10.1}, {-20.2, 20.2}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i8", api.Variable{
		Values:     int8(-10),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i8x1", api.Variable{
		Values:     []int8{-10},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i8x2", api.Variable{
		Values:     [][]int8{{-10, 10}, {-20, 20}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui8", api.Variable{
		Values:     uint8(10),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui8x1", api.Variable{
		Values:     []uint8{10},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui8x2", api.Variable{
		Values:     [][]uint8{{10, 20}, {20, 30}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i16", api.Variable{
		Values:     int16(-10000),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i16x1", api.Variable{
		Values:     []int16{-10000},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i16x2", api.Variable{
		Values:     [][]int16{{-10000, 10000}, {-20000, 20000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui16", api.Variable{
		Values:     uint16(10000),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui16x1", api.Variable{
		Values:     []uint16{10000},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui16x2", api.Variable{
		Values:     [][]uint16{{10000, 20000}, {20000, 30000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i32", api.Variable{
		Values:     int32(-10000000),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i32x1", api.Variable{
		Values:     []int32{-10000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i32x2", api.Variable{
		Values:     [][]int32{{-10000000, 10000000}, {-20000000, 20000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui32", api.Variable{
		Values:     uint32(10000000),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui32x1", api.Variable{
		Values:     []uint32{10000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui32x2", api.Variable{
		Values:     [][]uint32{{10000000, 20000000}, {20000000, 30000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i64", api.Variable{
		Values:     int64(-10000000000),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i64x1", api.Variable{
		Values:     []int64{-10000000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i64x2", api.Variable{
		Values:     [][]int64{{-10000000000, 10000000000}, {-20000000000, 20000000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui64", api.Variable{
		Values:     uint64(10000000000),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui64x1", api.Variable{
		Values:     []uint64{10000000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui64x2", api.Variable{
		Values:     [][]uint64{{10000000000, 20000000000}, {20000000000, 30000000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
}

var fvf32 = math.Float32frombits(0x7cf00000)
var fvf64 = math.Float64frombits(0x479e000000000000)

var fills = keyValList{
	{"str", api.Variable{
		Values:     "",
		Dimensions: nil,
		Attributes: nilMap}},
	{"strx1", api.Variable{
		Values:     []string{""},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"strx2", api.Variable{
		Values:     [][]string{{"", ""}, {"", ""}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"f32", api.Variable{
		Values:     float32(fvf32),
		Dimensions: nil,
		Attributes: nilMap}},
	{"f32x1", api.Variable{
		Values:     []float32{fvf32},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"f32x2", api.Variable{
		Values:     [][]float32{{fvf32, fvf32}, {fvf32, fvf32}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"f64", api.Variable{
		Values:     float64(fvf64),
		Dimensions: nil,
		Attributes: nilMap}},
	{"f64x1", api.Variable{
		Values:     []float64{fvf64},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"f64x2", api.Variable{
		Values:     [][]float64{{fvf64, fvf64}, {fvf64, fvf64}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i8", api.Variable{
		Values:     int8(-127),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i8x1", api.Variable{
		Values:     []int8{-127},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i8x2", api.Variable{
		Values:     [][]int8{{-127, -127}, {-127, -127}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui8", api.Variable{
		Values:     uint8(255),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui8x1", api.Variable{
		Values:     []uint8{255},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui8x2", api.Variable{
		Values:     [][]uint8{{255, 255}, {255, 255}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i16", api.Variable{
		Values:     int16(-32767),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i16x1", api.Variable{
		Values:     []int16{-32767},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i16x2", api.Variable{
		Values:     [][]int16{{-32767, -32767}, {-32767, -32767}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui16", api.Variable{
		Values:     uint16(65535),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui16x1", api.Variable{
		Values:     []uint16{65535},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui16x2", api.Variable{
		Values:     [][]uint16{{65535, 65535}, {65535, 65535}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i32", api.Variable{
		Values:     int32(-2147483647),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i32x1", api.Variable{
		Values:     []int32{-2147483647},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i32x2", api.Variable{
		Values:     [][]int32{{-2147483647, -2147483647}, {-2147483647, -2147483647}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui32", api.Variable{
		Values:     uint32(4294967295),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui32x1", api.Variable{
		Values:     []uint32{4294967295},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui32x2", api.Variable{
		Values:     [][]uint32{{4294967295, 4294967295}, {4294967295, 4294967295}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i64", api.Variable{
		Values:     int64(-9223372036854775806),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i64x1", api.Variable{
		Values:     []int64{-9223372036854775806},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i64x2", api.Variable{
		Values: [][]int64{
			{-9223372036854775806, -9223372036854775806},
			{-9223372036854775806, -9223372036854775806}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui64", api.Variable{
		Values:     uint64(18446744073709551614),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui64x1", api.Variable{
		Values:     []uint64{18446744073709551614},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui64x2", api.Variable{
		Values: [][]uint64{
			{18446744073709551614, 18446744073709551614},
			{18446744073709551614, 18446744073709551614}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
}

var fills2 = keyValList{
	{"str", api.Variable{
		Values:     "#",
		Dimensions: nil,
		Attributes: nilMap}},
	{"strx1", api.Variable{
		Values:     []string{"#"},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"strx2", api.Variable{
		Values:     [][]string{{"#", "#"}, {"#", "#"}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},

	{"f32", api.Variable{
		Values:     float32(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"f32x1", api.Variable{
		Values:     []float32{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"f32x2", api.Variable{
		Values:     [][]float32{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"f64", api.Variable{
		Values:     float64(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"f64x1", api.Variable{
		Values:     []float64{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"f64x2", api.Variable{
		Values:     [][]float64{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i8", api.Variable{
		Values:     int8(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i8x1", api.Variable{
		Values:     []int8{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i8x2", api.Variable{
		Values:     [][]int8{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui8", api.Variable{
		Values:     uint8(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui8x1", api.Variable{
		Values:     []uint8{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui8x2", api.Variable{
		Values:     [][]uint8{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i16", api.Variable{
		Values:     int16(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i16x1", api.Variable{
		Values:     []int16{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i16x2", api.Variable{
		Values:     [][]int16{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui16", api.Variable{
		Values:     uint16(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui16x1", api.Variable{
		Values:     []uint16{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui16x2", api.Variable{
		Values:     [][]uint16{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i32", api.Variable{
		Values:     int32(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i32x1", api.Variable{
		Values:     []int32{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i32x2", api.Variable{
		Values:     [][]int32{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui32", api.Variable{
		Values:     uint32(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui32x1", api.Variable{
		Values:     []uint32{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui32x2", api.Variable{
		Values:     [][]uint32{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"i64", api.Variable{
		Values:     int64(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"i64x1", api.Variable{
		Values:     []int64{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"i64x2", api.Variable{
		Values:     [][]int64{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
	{"ui64", api.Variable{
		Values:     uint64(0),
		Dimensions: nil,
		Attributes: nilMap}},
	{"ui64x1", api.Variable{
		Values:     []uint64{0},
		Dimensions: []string{"dim"},
		Attributes: nilMap}},
	{"ui64x2", api.Variable{
		Values:     [][]uint64{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap}},
}

func genIsNewer(genName string, srcName string) bool {
	gst, err := os.Stat(genName)
	if err != nil {
		return false
	}
	sst, err := os.Stat(srcName)
	if err != nil {
		return false
	}
	return gst.ModTime().After(sst.ModTime())
}

func ncGen(t *testing.T, fileNameNoExt string) string {
	t.Helper()
	srcName := "testdata/" + fileNameNoExt + ".cdl"
	genName := "testdata/" + fileNameNoExt + ".nc"
	if genIsNewer(genName, srcName) {
		return genName
	}
	cmd := exec.Command("ncgen", "-b", "-k", "hdf5", "-o", genName, "testdata/"+fileNameNoExt+".cdl")
	err := cmd.Run()
	if err != nil {
		t.Log("ncgen", "-b", "-k", "hdf5", "-o", genName, "testdata/"+fileNameNoExt+".cdl")
		t.Log(err)
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

func validate(t *testing.T, fname string, filters []string) {
	cmdString := []string{"-H", "-p", fname}
	cmd := exec.Command("h5dump", cmdString...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Error(err)
		return
	}
	err = cmd.Start()
	defer cmd.Wait()
	if err != nil {
		s := strings.Join(cmdString, " ")
		t.Error("h5dump", s, ":", err)
		return
	}
	out, err := ioutil.ReadAll(stdout)
	if err != nil {
		t.Error("ReadAll", err)
		return
	}
	filtMap := map[string]string{
		"SHUF":   "PREPROCESSING SHUFFLE",
		"FLET":   "CHECKSUM FLETCHER32",
		"GZIP=1": "COMPRESSION DEFLATE { LEVEL 1 }",
		"GZIP=5": "COMPRESSION DEFLATE { LEVEL 5 }",
		"GZIP=9": "COMPRESSION DEFLATE { LEVEL 9 }",
	}
	for _, f := range filters {
		if !strings.Contains(string(out), filtMap[f]) {

			t.Error(fname, "missing", f)
			t.Log(string(out))
			os.Remove(fname)
		}
	}
}

func ncFilter(t *testing.T, fileNameNoExt string, filters []string,
	extension string) string {
	t.Helper()
	genName := "testdata/" + fileNameNoExt + extension + ".nc"
	srcName := "testdata/" + fileNameNoExt + ".nc"
	if genIsNewer(genName, srcName) {
		return genName
	}

	// version 2 (latest) uses V4 data layout which is not supported yet
	// So use version 1.
	//cmdString := []string{"--low=1", "--high=1"}
	cmdString := []string{}
	for i := range filters {
		cmdString = append(cmdString, "-f", filters[i])
	}
	cmdString = append(cmdString, srcName, genName)
	cmd := exec.Command("h5repack", cmdString...)
	err := cmd.Run()
	if err != nil {
		s := strings.Join(cmdString, " ")
		t.Error("h5repack", s, ":", err)
		return ""
	}
	validate(t, genName, filters)
	return genName
}

func TestTypes(t *testing.T) {
	genName := ncGen(t, "testtypes")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkAll(t, nc, values)
}

func TestGlobalAttrs(t *testing.T) {
	genName := ncGen(t, "testattrs")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	exp, err := util.NewOrderedMap(
		[]string{"c", "str", "f32", "f64", "i8", "ui8", "i16", "ui16", "i32", "ui32", "i64", "ui64",
			"col", "all"},
		map[string]interface{}{
			"c":    "c",
			"str":  "hello",
			"f32":  float32(1),
			"f64":  float64(2),
			"i8":   int8(3),
			"ui8":  uint8(4),
			"i16":  int16(5),
			"ui16": uint16(6),
			"i32":  int32(7),
			"ui32": uint32(8),
			"i64":  int64(9),
			"ui64": uint64(10),
			"col":  enumerated{int8(3)},
			"all":  compound{int8('0'), int16(1), int32(2), float32(3), float64(4)},
		})
	if err != nil {
		t.Error(err)
		return
	}
	got := nc.Attributes()
	checkAllAttrs(t, got, exp)
	defer nc.Close()
}

func checkAllAttrs(t *testing.T, got api.AttributeMap, exp api.AttributeMap) {
	t.Helper()
	errors := false
	used := map[string]bool{}
	for _, key := range exp.Keys() {
		used[key] = true
		if !checkAttr(t, key, got, exp) {
			t.Error("values did not match")
			errors = true
		}
	}
	for _, key := range got.Keys() {
		if used[key] {
			continue
		}
		if !checkAttr(t, key, got, exp) {
			t.Error("values did not match")
			errors = true
		}
	}
	if errors {
		t.Logf("%#v", got)
	}
}

func TestFills(t *testing.T) {
	genName := ncGen(t, "testfills")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkAll(t, nc, fills)
}

func TestFills2(t *testing.T) {
	genName := ncGen(t, "testfills2")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkAllNoAttr(t, nc, fills2)
}

func TestGroups(t *testing.T) {
	genName := ncGen(t, "testgroups")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkGroup := func(nc api.Group, val []int32) {
		vr, err := nc.GetVariable("datamember")
		if err != nil {
			t.Error(err)
			return
		}
		if len(vr.Dimensions) != 1 {
			t.Error("dimension missing")
			return
		}
		if vr.Dimensions[0] != "d1" {
			t.Error("dimension name wrong", vr.Dimensions[0])
			return
		}
		x := vr.Values.([]int32)
		if len(x) != len(val) {
			t.Error("len x should be", len(val), "was", len(x), x, val)
			return
		}
		for i := range val {
			if x[i] != val[i] {
				t.Error("x[i] should be", val[i])
				return
			}
		}
	}
	checkGroup(nc, []int32{2})

	groups := nc.ListSubgroups()
	if groups[0] != "a" {
		t.Error("group a missing")
		return
	}
	nca, err := nc.GetGroup("a")
	if err != nil {
		t.Error(err)
		return
	}
	defer nca.Close()
	checkGroup(nca, []int32{3, 4})

	groups = nca.ListSubgroups()
	if groups[0] != "b" {
		t.Error("group a missing")
		return
	}
	ncb, err := nca.GetGroup("b")
	if err != nil {
		t.Error(err)
		return
	}
	defer ncb.Close()
	checkGroup(ncb, []int32{5, 6, 7})
}

func TestByte(t *testing.T) {
	genName := ncGen(t, "testbytes")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("b")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("should be no dimensions")
		return
	}
	b := vr.Values.(int8)
	if b != 1 {
		t.Error("value should be 1")
		return
	}

	vr, err = nc.GetVariable("ub")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("should be no dimensions")
		return
	}
	ub := vr.Values.(uint8)
	if ub != 2 {
		t.Error("value should be 2")
		return
	}

	vr, err = nc.GetVariable("ba")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 1 {
		t.Error("should be 1 dimension")
		return
	}
	ba := vr.Values.([]int8)
	if len(ba) != 1 {
		t.Error("dimension length should be 1")
	}
	if ba[0] != 3 {
		t.Error("value should be 3")
		return
	}

	vr, err = nc.GetVariable("uba")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 1 {
		t.Error("should be 1 dimension")
		return
	}
	uba := vr.Values.([]uint8)
	if len(uba) != 1 {
		t.Error("dimension length should be 1")
	}
	if uba[0] != 4 {
		t.Error("value should be 3")
		return
	}

	vr, err = nc.GetVariable("s")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("should be 0 dimensions")
		return
	}
	s := vr.Values.(string)
	if s != "5" {
		t.Error("value should be \"5\"")
		return
	}

	vr, err = nc.GetVariable("s2")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("should be 0 dimensions")
		return
	}
	s2 := vr.Values.(string)
	if s2 != "67" {
		t.Error("value should be \"67\"")
		return
	}

	// Scalar character is returned as a string, dimension 0
	vr, err = nc.GetVariable("c")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 0 {
		t.Error("should be 0 dimensions")
		return
	}
	c := vr.Values.(string)
	if c != "8" {
		t.Error("value should be \"8\"")
		return
	}

	// Character array is returned as a string (single character)
	vr, err = nc.GetVariable("c2")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 1 {
		t.Error("should be 1 dimensions")
		return
	}
	c2 := vr.Values.(string)
	if c2 != "9" {
		t.Error("value should be \"9\"")
		return
	}

	// Character array is returned as a string (multiple characters)
	vr, err = nc.GetVariable("c3")
	if err != nil {
		t.Error(err)
		return
	}
	if len(vr.Dimensions) != 1 {
		t.Error("should be 1 dimensions")
		return
	}
	c3 := vr.Values.(string)
	if c3 != "ab" {
		t.Error("value should be \"ab\"")
		return
	}
}

func TestCompound(t *testing.T) {
	genName := ncGen(t, "testcompounds")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	//SetLogLevel(util.LevelInfo)
	defer nc.Close()

	vals := []compound{
		{compound{
			int8('0'),
			int16(1),
			int32(2),
			float32(3.0),
			float64(4.0)},
			"a",
		},
		{compound{
			int8('1'),
			int16(2),
			int32(3),
			float32(4.0),
			float64(5.0)},
			"b",
		},
	}
	samevals := []compound{
		{int32(0), int32(1), int32(2)},
		{int32(3), int32(4), int32(5)},
	}
	values := keyValList{
		keyVal{"v",
			api.Variable{
				Values:     vals,
				Dimensions: []string{"dim"},
				Attributes: nilMap},
		},
		keyVal{"same",
			api.Variable{
				Values:     samevals,
				Dimensions: []string{"dim"},
				Attributes: nilMap},
		},
	}
	checkAll(t, nc, values)
}

func TestOneDim(t *testing.T) {
	// Set up
	fileName := "testonedim" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
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

}

func TestUnlimited(t *testing.T) {
	fileName := "testunlimited" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}

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
		{"i64x1", api.Variable{
			Values:     []int64{56100, 78100},
			Dimensions: []string{"d1"},
			Attributes: nilMap}},
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
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	empty := keyValList{
		{"a", api.Variable{
			Values:     []int32{},
			Dimensions: []string{"u"},
			Attributes: nilMap}},
		{"b", api.Variable{
			Values:     []opaque{},
			Dimensions: []string{"u"},
			Attributes: nilMap}},
		{"c", api.Variable{
			Values:     []compound{},
			Dimensions: []string{"u"},
			Attributes: nilMap}},
		{"d", api.Variable{
			Values:     [][]compound{},
			Dimensions: []string{"u", "u"},
			Attributes: nilMap}},
		{"e", api.Variable{
			Values:     [][][]compound{},
			Dimensions: []string{"u", "u", "u"},
			Attributes: nilMap}},
		{"f", api.Variable{
			Values:     [][]opaque{},
			Dimensions: []string{"u", "u"},
			Attributes: nilMap}},
		{"g", api.Variable{
			Values:     [][][]opaque{},
			Dimensions: []string{"u", "u", "u"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, empty)
}

func TestUnlimitedOnlyBytes(t *testing.T) {
	fileName := "testbytesunlimited" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
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

func testFilters(t *testing.T, filters []string, extension string) {
	t.Helper()
	fileName := "testfilters" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	filtName := ncFilter(t, fileName, filters, extension)
	if filtName == "" {
		t.Error(errorNcFilter)
		return
	}
	nc, err := Open(filtName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("nums")
	if err != nil {
		t.Error(err)
		return
	}
	nums := vr.Values.([]int32)
	if len(nums) != 10 {
		t.Error("wrong length")
		return
	}
	for i := range nums {
		if nums[i] != int32(i) {
			t.Error("wrong number")
			return
		}
	}
}

func TestFletcher32(t *testing.T) {
	testFilters(t, []string{"FLET"}, "-f")
}

func TestShuffle(t *testing.T) {
	testFilters(t, []string{"SHUF"}, "-s")
}

func TestGzip(t *testing.T) {
	testFilters(t, []string{"GZIP=1"}, "-g1")
	testFilters(t, []string{"GZIP=5"}, "-g5")
	testFilters(t, []string{"GZIP=9"}, "-g9")
}

func TestCombinedFilters(t *testing.T) {
	// FLET, if present, must always be last.
	// If GZIP is present, SHUF may appear only before it.
	testFilters(t, []string{"SHUF", "FLET"}, "-sf")
	testFilters(t, []string{"SHUF", "GZIP=5"}, "-sg5")
	testFilters(t, []string{"GZIP=5", "FLET"}, "-g5f")
	// This should be okay
	testFilters(t, []string{"SHUF", "GZIP=5", "FLET"}, "-sg5f")
}

func TestBadMagic(t *testing.T) {
	//SetLogLevel(util.LevelInfo)
	fileName := "testdata/badmagic" // base filename without extension
	_, err := Open(fileName)
	if err != ErrBadMagic {
		t.Error("bad magic error not seen")
		return
	}
}

func TestUnlimitedOnlyShorts(t *testing.T) {
	fileName := "testshortsonly" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
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

func TestOpaque(t *testing.T) {
	fileName := "testopaque" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	opaque := keyValList{
		{"v", api.Variable{
			Values: []opaque{
				{0xde, 0xad, 0xbe, 0xef, 0x01},
				{0xde, 0xad, 0xbe, 0xef, 0x02},
				{0xde, 0xad, 0xbe, 0xef, 0x03},
				{0xde, 0xad, 0xbe, 0xef, 0x04},
				{0xde, 0xad, 0xbe, 0xef, 0x05}},
			Dimensions: []string{"dim"},
			Attributes: nilMap}},
	}
	checkAll(t, nc, opaque)
}

func TestEnum(t *testing.T) {
	fileName := "testenum" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	enum := keyValList{
		{"c", api.Variable{
			Values:     enumerated{[]int8{0, 1, 2, 3, 4, 5}},
			Dimensions: []string{"dim"},
			Attributes: nilMap}},
		{"nodim", api.Variable{
			Values:     enumerated{int8(2)},
			Dimensions: nil,
			Attributes: nilMap}},
	}
	checkAll(t, nc, enum)
}

func TestNCProperties(t *testing.T) {
	fileName := "testenum" // base filename without extension
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	nc, err := Open(genName)
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
	if len(spl) < 3 || spl[0] != "version=2" {
		t.Error("1. Hidden property is not correct value:", hidden)
		return
	}
	if !strings.HasPrefix(spl[1], "netcdf=") {
		t.Error("2. Hidden property is not correct value:", hidden)
		return
	}
	if !strings.HasPrefix(spl[2], "hdf5=") {
		t.Error("3. Hidden property is not correct value:", hidden)
		return
	}
}

func (kl keyValList) check(t *testing.T, name string, val api.Variable) bool {
	t.Helper()
	var kv *keyVal
	for _, k := range kl {
		if name != k.name {
			continue
		}
		kv = &k
		break
	}
	if kv == nil {
		t.Log(name, "not found")
		return false
	}
	if !reflect.DeepEqual(val.Values, kv.val.Values) {
		t.Logf("var deepequal name=%s got=%#v exp=%#v", name,
			val.Values, kv.val.Values)
		t.Logf("types got type=%T exp type=%T", val.Values, kv.val.Values)
		return false
	}
	checkAllAttrs(t, val.Attributes, kv.val.Attributes)
	if !reflect.DeepEqual(val.Dimensions, kv.val.Dimensions) &&
		!(len(val.Dimensions) == 0 && len(kv.val.Dimensions) == 0) {
		t.Log("dims", name, val.Dimensions, kv.val.Dimensions)
		return false
	}
	return true
}

func checkAttr(t *testing.T, name string, gotAttr api.AttributeMap, expAttr api.AttributeMap) bool {
	t.Helper()
	exp, has := expAttr.Get(name)
	if !has {
		t.Error("Expected attribute", name, "not found")
		return false
	}
	got, has := gotAttr.Get(name)
	if !has {
		t.Error("Received attribute", name, "not found")
		return false
	}
	if !reflect.DeepEqual(got, exp) {
		t.Logf("var deepequal name=%s got=%#v exp=%#v", name, got, exp)
		t.Logf("types got type=%T exp type=%T", got, exp)
		return false
	}
	return true
}

func checkAllAttrOption(t *testing.T, nc api.Group, values keyValList, hasAttr bool) {
	t.Helper()
	vars := nc.ListVariables()
	if len(vars) != len(values) {
		t.Error("mismatch", len(vars), len(values), vars, values)
		return
	}
	for _, name := range vars {
		vr, err := nc.GetVariable(name)
		if err != nil {
			t.Error(err)
			return
		}
		if !hasAttr {
			vr.Attributes = nilMap
		}
		if !values.check(t, name, *vr) {
			t.Error("mismatch")
		}
	}
}

func checkAllNoAttr(t *testing.T, nc api.Group, values keyValList) {
	t.Helper()
	checkAllAttrOption(t, nc, values, false /*attributes*/)
}

func checkAll(t *testing.T, nc api.Group, values keyValList) {
	t.Helper()
	checkAllAttrOption(t, nc, values, true /*attributes*/)
}
