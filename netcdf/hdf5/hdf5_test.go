package hdf5

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

const (
	errorNcGen    = "Error running ncgen command from netcdf package"
	errorNcFilter = "Error running h5repack command from hdf5 package"
)

const (
	dontDoAttrs = false
	doAttrs     = true
)

const (
	ncGenBinary    = "ncgen"
	h5DumpBinary   = "h5dump"
	h5RepackBinary = "h5repack"
)

type keyVal struct {
	name     string
	baseType string // not including dimensions
	val      api.Variable
}

type keyValList []keyVal

var nilMap, _ = newTypedAttributeMap(nil, []string{}, map[string]any{})

var values = keyValList{
	{"str", "string", api.Variable{
		Values:     "a",
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"strx1", "string", api.Variable{
		Values:     []string{"a"},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"strx2", "string", api.Variable{
		Values:     [][]string{{"ab", "cd"}, {"ef", "gh"}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"f32", "float", api.Variable{
		Values:     float32(-10.1),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"f32x1", "float", api.Variable{
		Values:     []float32{-10.1},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"f32x2", "float", api.Variable{
		Values:     [][]float32{{-10.1, 10.1}, {-20.2, 20.2}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"f64", "double", api.Variable{
		Values:     float64(-10.1),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"f64x1", "double", api.Variable{
		Values:     []float64{-10.1},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"f64x2", "double", api.Variable{
		Values:     [][]float64{{-10.1, 10.1}, {-20.2, 20.2}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i8", "byte", api.Variable{
		Values:     int8(-10),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i8x1", "byte", api.Variable{
		Values:     []int8{-10},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i8x2", "byte", api.Variable{
		Values:     [][]int8{{-10, 10}, {-20, 20}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui8", "ubyte", api.Variable{
		Values:     uint8(10),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui8x1", "ubyte", api.Variable{
		Values:     []uint8{10},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui8x2", "ubyte", api.Variable{
		Values:     [][]uint8{{10, 20}, {20, 30}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i16", "short", api.Variable{
		Values:     int16(-10000),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i16x1", "short", api.Variable{
		Values:     []int16{-10000},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i16x2", "short", api.Variable{
		Values:     [][]int16{{-10000, 10000}, {-20000, 20000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui16", "ushort", api.Variable{
		Values:     uint16(10000),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui16x1", "ushort", api.Variable{
		Values:     []uint16{10000},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui16x2", "ushort", api.Variable{
		Values:     [][]uint16{{10000, 20000}, {20000, 30000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i32", "int", api.Variable{
		Values:     int32(-10000000),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i32x1", "int", api.Variable{
		Values:     []int32{-10000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i32x2", "int", api.Variable{
		Values:     [][]int32{{-10000000, 10000000}, {-20000000, 20000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui32", "uint", api.Variable{
		Values:     uint32(10000000),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui32x1", "uint", api.Variable{
		Values:     []uint32{10000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui32x2", "uint", api.Variable{
		Values:     [][]uint32{{10000000, 20000000}, {20000000, 30000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i64", "int64", api.Variable{
		Values:     int64(-10000000000),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i64x1", "int64", api.Variable{
		Values:     []int64{-10000000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i64x2", "int64", api.Variable{
		Values:     [][]int64{{-10000000000, 10000000000}, {-20000000000, 20000000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui64", "uint64", api.Variable{
		Values:     uint64(10000000000),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui64x1", "uint64", api.Variable{
		Values:     []uint64{10000000000},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui64x2", "uint64", api.Variable{
		Values:     [][]uint64{{10000000000, 20000000000}, {20000000000, 30000000000}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
}

var (
	fvf32 = math.Float32frombits(0x7cf00000)
	fvf64 = math.Float64frombits(0x479e000000000000)
)

var fills = keyValList{
	{"str", "string", api.Variable{
		Values:     "",
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"strx1", "string", api.Variable{
		Values:     []string{""},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"strx2", "string", api.Variable{
		Values:     [][]string{{"", ""}, {"", ""}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"f32", "float", api.Variable{
		Values:     float32(fvf32),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"f32x1", "float", api.Variable{
		Values:     []float32{fvf32},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"f32x2", "float", api.Variable{
		Values:     [][]float32{{fvf32, fvf32}, {fvf32, fvf32}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"f64", "double", api.Variable{
		Values:     float64(fvf64),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"f64x1", "double", api.Variable{
		Values:     []float64{fvf64},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"f64x2", "double", api.Variable{
		Values:     [][]float64{{fvf64, fvf64}, {fvf64, fvf64}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i8", "byte", api.Variable{
		Values:     int8(-127),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i8x1", "byte", api.Variable{
		Values:     []int8{-127},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i8x2", "byte", api.Variable{
		Values:     [][]int8{{-127, -127}, {-127, -127}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui8", "ubyte", api.Variable{
		Values:     uint8(255),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui8x1", "ubyte", api.Variable{
		Values:     []uint8{255},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui8x2", "ubyte", api.Variable{
		Values:     [][]uint8{{255, 255}, {255, 255}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i16", "short", api.Variable{
		Values:     int16(-32767),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i16x1", "short", api.Variable{
		Values:     []int16{-32767},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i16x2", "short", api.Variable{
		Values:     [][]int16{{-32767, -32767}, {-32767, -32767}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui16", "ushort", api.Variable{
		Values:     uint16(65535),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui16x1", "ushort", api.Variable{
		Values:     []uint16{65535},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui16x2", "ushort", api.Variable{
		Values:     [][]uint16{{65535, 65535}, {65535, 65535}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i32", "int", api.Variable{
		Values:     int32(-2147483647),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i32x1", "int", api.Variable{
		Values:     []int32{-2147483647},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i32x2", "int", api.Variable{
		Values:     [][]int32{{-2147483647, -2147483647}, {-2147483647, -2147483647}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui32", "uint", api.Variable{
		Values:     uint32(4294967295),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui32x1", "uint", api.Variable{
		Values:     []uint32{4294967295},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui32x2", "uint", api.Variable{
		Values:     [][]uint32{{4294967295, 4294967295}, {4294967295, 4294967295}},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"i64", "int64", api.Variable{
		Values:     int64(-9223372036854775806),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"i64x1", "int64", api.Variable{
		Values:     []int64{-9223372036854775806},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"i64x2", "int64", api.Variable{
		Values: [][]int64{
			{-9223372036854775806, -9223372036854775806},
			{-9223372036854775806, -9223372036854775806},
		},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
	{"ui64", "uint64", api.Variable{
		Values:     uint64(18446744073709551614),
		Dimensions: nil,
		Attributes: nilMap,
	}},
	{"ui64x1", "uint64", api.Variable{
		Values:     []uint64{18446744073709551614},
		Dimensions: []string{"dim"},
		Attributes: nilMap,
	}},
	{"ui64x2", "uint64", api.Variable{
		Values: [][]uint64{
			{18446744073709551614, 18446744073709551614},
			{18446744073709551614, 18446744073709551614},
		},
		Dimensions: []string{"d1", "d2"},
		Attributes: nilMap,
	}},
}

var fills2 = keyValList{
	{"str", "string", api.Variable{
		Values:     "#",
		Dimensions: nil,
		Attributes: makeFill("#"),
	}},
	{"strx1", "string", api.Variable{
		Values:     []string{"#"},
		Dimensions: []string{"dim"},
		Attributes: makeFill("#"),
	}},
	{"strx2", "string", api.Variable{
		Values:     [][]string{{"#", "#"}, {"#", "#"}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill("#"),
	}},
	{"f32", "float", api.Variable{
		Values:     float32(0),
		Dimensions: nil,
		Attributes: makeFill(float32(0)),
	}},
	{"f32x1", "float", api.Variable{
		Values:     []float32{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(float32(0)),
	}},
	{"f32x2", "float", api.Variable{
		Values:     [][]float32{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(float32(0)),
	}},
	{"f64", "double", api.Variable{
		Values:     float64(0),
		Dimensions: nil,
		Attributes: makeFill(float64(0)),
	}},
	{"f64x1", "double", api.Variable{
		Values:     []float64{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(float64(0)),
	}},
	{"f64x2", "double", api.Variable{
		Values:     [][]float64{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(float64(0)),
	}},
	{"i8", "byte", api.Variable{
		Values:     int8(0),
		Dimensions: nil,
		Attributes: makeFill(int8(0)),
	}},
	{"i8x1", "byte", api.Variable{
		Values:     []int8{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(int8(0)),
	}},
	{"i8x2", "byte", api.Variable{
		Values:     [][]int8{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(int8(0)),
	}},
	{"ui8", "ubyte", api.Variable{
		Values:     uint8(0),
		Dimensions: nil,
		Attributes: makeFill(uint8(0)),
	}},
	{"ui8x1", "ubyte", api.Variable{
		Values:     []uint8{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(uint8(0)),
	}},
	{"ui8x2", "ubyte", api.Variable{
		Values:     [][]uint8{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(uint8(0)),
	}},
	{"i16", "short", api.Variable{
		Values:     int16(0),
		Dimensions: nil,
		Attributes: makeFill(int16(0)),
	}},
	{"i16x1", "short", api.Variable{
		Values:     []int16{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(int16(0)),
	}},
	{"i16x2", "short", api.Variable{
		Values:     [][]int16{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(int16(0)),
	}},
	{"ui16", "ushort", api.Variable{
		Values:     uint16(0),
		Dimensions: nil,
		Attributes: makeFill(uint16(0)),
	}},
	{"ui16x1", "ushort", api.Variable{
		Values:     []uint16{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(uint16(0)),
	}},
	{"ui16x2", "ushort", api.Variable{
		Values:     [][]uint16{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(uint16(0)),
	}},
	{"i32", "int", api.Variable{
		Values:     int32(0),
		Dimensions: nil,
		Attributes: makeFill(int32(0)),
	}},
	{"i32x1", "int", api.Variable{
		Values:     []int32{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(int32(0)),
	}},
	{"i32x2", "int", api.Variable{
		Values:     [][]int32{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(int32(0)),
	}},
	{"ui32", "uint", api.Variable{
		Values:     uint32(0),
		Dimensions: nil,
		Attributes: makeFill(uint32(0)),
	}},
	{"ui32x1", "uint", api.Variable{
		Values:     []uint32{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(uint32(0)),
	}},
	{"ui32x2", "uint", api.Variable{
		Values:     [][]uint32{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(uint32(0)),
	}},
	{"i64", "int64", api.Variable{
		Values:     int64(0),
		Dimensions: nil,
		Attributes: makeFill(int64(0)),
	}},
	{"i64x1", "int64", api.Variable{
		Values:     []int64{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(int64(0)),
	}},
	{"i64x2", "int64", api.Variable{
		Values:     [][]int64{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(int64(0)),
	}},
	{"ui64", "uint64", api.Variable{
		Values:     uint64(0),
		Dimensions: nil,
		Attributes: makeFill(uint64(0)),
	}},
	{"ui64x1", "uint64", api.Variable{
		Values:     []uint64{0},
		Dimensions: []string{"dim"},
		Attributes: makeFill(uint64(0)),
	}},
	{"ui64x2", "uint64", api.Variable{
		Values:     [][]uint64{{0, 0}, {0, 0}},
		Dimensions: []string{"d1", "d2"},
		Attributes: makeFill(uint64(0)),
	}},
}

func makeFill(fill any) api.AttributeMap {
	ret, _ := newTypedAttributeMap(nil, []string{"_FillValue"},
		map[string]any{"_FillValue": fill})
	return ret
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
	cmd := exec.Command(ncGenBinary, "-b", "-k", "hdf5", "-o", genName, "testdata/"+fileNameNoExt+".cdl")
	err := cmd.Run()
	if err != nil {
		t.Log(ncGenBinary, "-b", "-k", "hdf5", "-o", genName, "testdata/"+fileNameNoExt+".cdl")
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
	cmd := exec.Command(h5DumpBinary, cmdString...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Error(err)
		return
	}
	err = cmd.Start()
	defer cmd.Wait()
	if err != nil {
		s := strings.Join(cmdString, " ")
		t.Error(h5DumpBinary, s, ":", err)
		return
	}
	out, err := io.ReadAll(stdout)
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
	// cmdString := []string{"--low=1", "--high=1"}
	cmdString := []string{}
	for i := range filters {
		cmdString = append(cmdString, "-f", filters[i])
	}
	cmdString = append(cmdString, srcName, genName)
	cmd := exec.Command(h5RepackBinary, cmdString...)
	err := cmd.Run()
	if err != nil {
		s := strings.Join(cmdString, " ")
		t.Error(h5RepackBinary, s, ":", err)
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
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	checkAll(t, nc, values)
}

// Big-endian test. Writes a big-endian file with our writer and reads it back.
func TestTypesBE(t *testing.T) {
	// Filter to numeric types only (skip strings)
	var values2 keyValList
	for i := range values {
		if strings.HasPrefix(values[i].name, "str") {
			continue
		}
		values2 = append(values2, values[i])
	}

	// Write a big-endian file
	fileName := "testdata/testtypesbe_out.nc"
	w, err := OpenWriter(fileName)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer os.Remove(fileName)
	w.(*HDF5Writer).byteOrder = binary.BigEndian
	for _, v := range values2 {
		err = w.AddVar(v.name, api.Variable{
			Values:     v.val.Values,
			Dimensions: v.val.Dimensions,
			Attributes: filterAttributes(v.val.Attributes),
		})
		if err != nil {
			t.Fatalf("AddVar(%s): %v", v.name, err)
		}
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read it back and verify
	nc, err := Open(fileName)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer nc.Close()
	checkAll(t, nc, values2)
}

func TestGlobalAttrs(t *testing.T) {
	genName := ncGen(t, "testattrs")
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
	exp, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{
			"c", "str", "f32", "f64", "i8", "ui8", "i16", "ui16", "i32", "ui32", "i64", "ui64",
			"col", "all",
		},
		map[string]any{
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
			"col": enumerated{
				values:     int8(3),
				names:      []string{"RED", "YELLOW", "GREEN", "CYAN", "BLUE", "MAGENTA"},
				memberVals: []any{int8(0), int8(1), int8(2), int8(3), int8(4), int8(5)},
			},
			"all": compound{
				{"b", int8('0')},
				{"s", int16(1)},
				{"i", int32(2)},
				{"f", float32(3)},
				{"d", float64(4)},
			},
		})
	if err != nil {
		t.Error(err)
		return
	}
	got := nc.Attributes()
	checkAllAttrs(t, "<global>", got, exp)

	gTypes := map[string]any{
		"c":    "string",
		"str":  "string",
		"f32":  "float",
		"f64":  "double",
		"i8":   "byte",
		"ui8":  "ubyte",
		"i16":  "short",
		"ui16": "ushort",
		"i32":  "int",
		"ui32": "uint",
		"i64":  "int64",
		"ui64": "uint64",
		"col":  "color",
		"all":  "alltypes",
	}

	for _, a := range got.Keys() {
		ty, has := nc.Attributes().GetType(a)
		if !has {
			t.Error("can't find type for attribute", a)
		}
		exp, has := gTypes[a]
		if has {
			if exp != ty {
				t.Error("global attr", a, "type got=", ty, "exp=", exp)
			}
		} else {
			t.Error("type not found for", a)
		}
	}

	// test global attributes of subgroups...
	snc, _ := nc.GetGroup("subgroup")
	got = snc.Attributes()
	_, has := got.Get("groupattr")
	if !has {
		t.Error("Could not find subgroup attribute")
	}
}

func checkAllAttrs(t *testing.T, name string, got api.AttributeMap, exp api.AttributeMap) {
	t.Helper()
	errors := false
	used := map[string]bool{}
	for _, key := range exp.Keys() {
		used[key] = true
		if !checkAttr(t, key, got, exp) {
			g, _ := got.Get(key)
			e, _ := exp.Get(key)
			t.Error(name, "values did not match", key, "(in exp)",
				"got=", g, "exp=", e)
			errors = true
		}
	}
	for _, key := range got.Keys() {
		if used[key] {
			continue
		}
		if !checkAttr(t, key, got, exp) {
			g, _ := got.Get(key)
			e, _ := exp.Get(key)
			t.Error(name, "values did not match", key, "(in got)", "got=", g, "exp=", e)
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
	defer os.Remove(genName)
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
	defer os.Remove(genName)
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
	defer os.Remove(genName)
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
	if groups == nil {
		t.Error("groups  missing")
		return
	}
	if groups[0] != "a" {
		t.Error("group a missing")
		return
	}
	vr, err := nc.GetVariable("a")
	if err == nil {
		t.Error("groups are not variables", vr)
		return
	}
	ty, has := nc.GetGoType("a")
	if has {
		t.Error("groups are not go types", ty)
		return
	}
	ty, has = nc.GetType("a")
	if has {
		t.Error("groups are not types", ty)
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

	// Try lookups from root
	names := []string{"/", "/a", "/a/b", "a", "a/b"}
	for _, name := range names {
		_, err = nc.GetGroup(name)
		if err != nil {
			t.Error("GetGroup", name, err)
			return
		}
	}
	// Try lookup from a
	names = []string{"/a/b", "b"}
	for _, name := range names {
		_, err = nca.GetGroup(name)
		if err != nil {
			t.Error("GetGroup", name, err)
			return
		}
	}
	// Try not found
	names = []string{"/c", "c"}
	for _, name := range names {
		_, err = nca.GetGroup(name)
		if err != ErrNotFound {
			t.Error("GetGroup", name, err)
			return
		}
	}
}

func TestByte(t *testing.T) {
	genName := ncGen(t, "testbytes")
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
		t.Error("got=", c3, "exp=", "ab")
		return
	}
}

func TestCompound(t *testing.T) {
	genName := ncGen(t, "testcompounds")
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

	vals := []compound{
		{
			compoundField{
				Name: "A",
				Val: compound{
					{"B", int8('0')},
					{"S", int16(1)},
					{"I", int32(2)},
					{"F", float32(3.0)},
					{"D", float64(4.0)},
				},
			},
			compoundField{
				Name: "S",
				Val:  "a",
			},
		},
		{
			compoundField{
				Name: "A",
				Val: compound{
					{"B", int8('1')},
					{"S", int16(2)},
					{"I", int32(3)},
					{"F", float32(4.0)},
					{"D", float64(5.0)},
				},
			},
			compoundField{
				Name: "S",
				Val:  "b",
			},
		},
	}
	samevals := []compound{
		{{"A", int32(0)}, {"B", int32(1)}, {"C", int32(2)}},
		{{"A", int32(3)}, {"B", int32(4)}, {"C", int32(5)}},
	}
	values := keyValList{
		keyVal{
			"v", "Includes",
			api.Variable{
				Values:     vals,
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
		keyVal{
			"same", "Sametypes",
			api.Variable{
				Values:     samevals,
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
	}
	checkAll(t, nc, values)
}

func TestCompound2(t *testing.T) {
	genName := ncGen(t, "testcompounds")
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
	type Sametypes struct {
		A int32
		B int32
		C int32
	}
	type Alltypes struct {
		B int8
		S int16
		I int32
		F float32
		D float64
	}
	type Includes struct {
		A Alltypes
		S string
	}
	var st Sametypes
	var at Alltypes
	var in Includes
	h5 := nc.(*HDF5)
	h5.register("Alltypes", at)
	h5.register("Sametypes", st)
	h5.register("Includes", in)

	vals := []Includes{
		{
			Alltypes{int8('0'), int16(1), int32(2), float32(3.0), float64(4.0)},
			string("a"),
		},
		{
			Alltypes{int8('1'), int16(2), int32(3), float32(4.0), float64(5.0)},
			string("b"),
		},
	}
	samevals := []Sametypes{
		{int32(0), int32(1), int32(2)},
		{int32(3), int32(4), int32(5)},
	}
	values := keyValList{
		keyVal{
			"v", "Includes",
			api.Variable{
				Values:     vals,
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
		keyVal{
			"same", "Sametypes",
			api.Variable{
				Values:     samevals,
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
	}
	checkAll(t, nc, values)
}

// TestCompoundDatatypeV4 tests reading compound types encoded with datatype
// version 4, which is used by HDF5 1.12+ (H5F_LIBVER_V112). We create a
// fixed-dimension compound file and repack it with --low=3 --high=3 to force
// V112 format, which encodes datatypes at version 4. Fixed dimensions are
// used because V112 also forces V4 data layout messages for chunked data
// (UNLIMITED dimensions), which the reader does not yet support.
func TestCompoundDatatypeV4(t *testing.T) {
	genName := ncGen(t, "testcompoundsv4")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)

	// Repack with V112 library version bounds to produce datatype version 4
	repackedName := "testdata/testcompoundsv4_repacked.nc"
	cmd := exec.Command(h5RepackBinary, "--low=3", "--high=3", genName, repackedName)
	err := cmd.Run()
	if err != nil {
		t.Skip("h5repack with --low=3 --high=3 not supported:", err)
		return
	}
	defer os.Remove(repackedName)

	nc, err := Open(repackedName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()

	vals := []compound{
		{
			compoundField{
				Name: "A",
				Val: compound{
					{"B", int8('0')},
					{"S", int16(1)},
					{"I", int32(2)},
					{"F", float32(3.0)},
					{"D", float64(4.0)},
				},
			},
			compoundField{
				Name: "S",
				Val:  "a",
			},
		},
		{
			compoundField{
				Name: "A",
				Val: compound{
					{"B", int8('1')},
					{"S", int16(2)},
					{"I", int32(3)},
					{"F", float32(4.0)},
					{"D", float64(5.0)},
				},
			},
			compoundField{
				Name: "S",
				Val:  "b",
			},
		},
	}
	samevals := []compound{
		{{"A", int32(0)}, {"B", int32(1)}, {"C", int32(2)}},
		{{"A", int32(3)}, {"B", int32(4)}, {"C", int32(5)}},
	}
	values := keyValList{
		keyVal{
			"v", "Includes",
			api.Variable{
				Values:     vals,
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
		keyVal{
			"same", "Sametypes",
			api.Variable{
				Values:     samevals,
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
	}
	checkAll(t, nc, values)
}

func TestArray(t *testing.T) {
	// Arrays are only used implicitly in NetCDF4.
	genName := ncGen(t, "testarray")
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

	values := keyValList{
		keyVal{
			"c", "comp",
			api.Variable{
				Values: []compound{
					{
						compoundField{
							Name: "iArray",
							Val:  []int32{1, 2, 3},
						},
						compoundField{
							Name: "fArray",
							Val:  [][]float32{{4, 5, 6}, {7, 8, 9}},
						},
					},
					{
						compoundField{
							Name: "iArray",
							Val:  []int32{10, 11, 12},
						},
						compoundField{
							Name: "fArray",
							Val:  [][]float32{{13, 14, 15}, {16, 17, 18}},
						},
					},
				},
				Dimensions: []string{"dim"},
				Attributes: nilMap,
			},
		},
	}
	checkAll(t, nc, values)
}

func TestSimple(t *testing.T) {
	genName := ncGen(t, "testsimple")
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

	values := keyValList{
		keyVal{
			"anA", "AAA",
			api.Variable{
				Values: compound{
					{Name: "s", Val: int16(55)},
					{Name: "i", Val: int32(5280)},
				},
				Dimensions: []string{},
				Attributes: nilMap,
			},
		},
		keyVal{
			"aB", "BBB",
			api.Variable{
				Values: compound{
					{Name: "x", Val: float32(98.6)},
					{Name: "y", Val: float64(-273.3)},
				},
				Dimensions: []string{},
				Attributes: nilMap,
			},
		},
		keyVal{
			"scalar", "int",
			api.Variable{
				Values:     int32(5),
				Dimensions: []string{},
				Attributes: nilMap,
			},
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
	defer os.Remove(genName)
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
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i8x1", "byte", api.Variable{
			Values:     [][]int8{{12}, {56}},
			Dimensions: []string{"d1", "d2"},
			Attributes: nilMap,
		}},
		{"i16x1", "short", api.Variable{
			Values:     []int16{9876, 5432},
			Dimensions: []string{"d1"},
			Attributes: nilMap,
		}},
		{"i32x1", "int", api.Variable{
			Values:     []int32{12, 34},
			Dimensions: []string{"d1"},
			Attributes: nilMap,
		}},
		{"i64x1", "int64", api.Variable{
			Values:     []int64{56100, 78100},
			Dimensions: []string{"d1"},
			Attributes: nilMap,
		}},
	}
	checkAll(t, nc, unlims)
}

func TestVariableLength(t *testing.T) {
	fileName := "testvlen" // base filename without extension
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

	trickyAttrs, err := newTypedAttributeMap(nc.(*HDF5), []string{"Tricky"},
		map[string]any{
			"Tricky": compound{
				{Name: "trickyInt", Val: int32(1)},
				{Name: "trickVlen", Val: [][]compoundField{
					{
						// ncgen has a bug and will generate bad numbers here
						{Name: "firstEasy", Val: int32(2)},
						{Name: "secondEasy", Val: int32(3)},
					},

					// same bug will mess this up also
					//{int32(-295104496), int32(22052)}
					{
						{Name: "firstEasy", Val: int32(4)},
						{Name: "secondEasy", Val: int32(5)},
					},
					{
						// This one is okay though
						{Name: "firstEasy", Val: int32(6)},
						{Name: "secondEasy", Val: int32(7)},
					},
				}},
			},
		})
	if err != nil {
		t.Error(err)
		return
	}

	vintAttrs, err := newTypedAttributeMap(nc.(*HDF5), []string{"Vint"},
		map[string]any{
			"Vint": [][]int32{
				{}, // zero
				{1},
				{2, 3},
				{4, 5, 6},
				{7, 8, 9, 10},
				{11, 12, 13, 14, 15},
			},
		})
	if err != nil {
		t.Error(err)
		return
	}
	tricky, _ := trickyAttrs.Get("Tricky")
	vint, _ := vintAttrs.Get("Vint")
	vars := keyValList{
		{"v", "vint", api.Variable{
			Values: [][]int32{
				{}, // zero
				{1},
				{2, 3},
				{4, 5, 6},
				{7, 8, 9, 10},
				{11, 12, 13, 14, 15},
			},
			Dimensions: []string{"dim"},
			Attributes: trickyAttrs,
		}},
		{"v2", "vint", api.Variable{
			Values: [][]int32{
				{11, 12, 13, 14, 15},
				{7, 8, 9, 10},
				{4, 5, 6},
				{2, 3},
				{1},
				{}, // zero
			},
			Dimensions: []string{"dim"},
			Attributes: vintAttrs,
		}},
	}
	checkAllNoAttr(t, nc, vars)
	expAttrs, err := newTypedAttributeMap(nc.(*HDF5), []string{"Tricky", "Vint"},
		map[string]any{
			"Tricky": tricky,
			"Vint":   vint,
		})
	if err != nil {
		t.Error(err)
		return
	}
	got := nc.Attributes()
	ncGenBug := true
	if !ncGenBug {
		checkAllAttrs(t, "<TestVariableLength>", got, expAttrs)
	} else {
		// tricky, _ := got.Get("Tricky")
		vint, _ := got.Get("Vint")
		got, _ = util.NewOrderedMap(
			[]string{"Tricky", "Vint"},
			map[string]any{
				"Tricky": tricky,
				"Vint":   vint,
			})
		checkAllAttrs(t, "<TestVariableLength>", got, expAttrs)
	}
}

func TestVariableLength2(t *testing.T) {
	fileName := "testvlen" // base filename without extension
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

	type vint []int32
	type Easy struct {
		FirstEasy  int32
		SecondEasy int32
	}
	type EasyVlen []Easy
	type Trickyt struct {
		TrickyInt int32
		TrickVlen EasyVlen
	}
	var v vint
	var e Easy
	var ev EasyVlen
	var tt Trickyt
	h5 := nc.(*HDF5)
	h5.register("vint", v)
	h5.register("Easy", e)
	h5.register("EasyVlen", ev)
	h5.register("Tricky_t", tt)
	trickyAttrs, err := newTypedAttributeMap(nc.(*HDF5), []string{"Tricky"},
		map[string]any{
			"Tricky": Trickyt{
				int32(1),
				EasyVlen{
					{int32(2), int32(3)},
					{int32(4), int32(5)},
					{int32(6), int32(7)},
				},
			},
		})
	if err != nil {
		t.Error(err)
		return
	}

	vintAttrs, err := newTypedAttributeMap(nc.(*HDF5), []string{"Vint"},
		map[string]any{
			"Vint": []vint{
				{}, // zero
				{1},
				{2, 3},
				{4, 5, 6},
				{7, 8, 9, 10},
				{11, 12, 13, 14, 15},
			},
		})
	if err != nil {
		t.Error(err)
		return
	}
	tricky, _ := trickyAttrs.Get("Tricky")
	vnt, _ := vintAttrs.Get("Vint")
	vars := keyValList{
		{"v", "vint", api.Variable{
			Values: []vint{
				{}, // zero
				{1},
				{2, 3},
				{4, 5, 6},
				{7, 8, 9, 10},
				{11, 12, 13, 14, 15},
			},
			Dimensions: []string{"dim"},
			Attributes: trickyAttrs,
		}},
		{"v2", "vint", api.Variable{
			Values: []vint{
				{11, 12, 13, 14, 15},
				{7, 8, 9, 10},
				{4, 5, 6},
				{2, 3},
				{1},
				{}, // zero
			},
			Dimensions: []string{"dim"},
			Attributes: vintAttrs,
		}},
	}
	checkAllNoAttr(t, nc, vars)
	expAttrs, err := newTypedAttributeMap(nc.(*HDF5), []string{"Tricky", "Vint"},
		map[string]any{
			"Tricky": tricky,
			"Vint":   vnt,
		})
	if err != nil {
		t.Error(err)
		return
	}
	got := nc.Attributes()
	ncGenBug := true
	if !ncGenBug {
		checkAllAttrs(t, "<TestVariableLength>", got, expAttrs)
	} else {
		// tricky, _ := got.Get("Tricky")
		vnt, _ := got.Get("Vint")
		got, _ = util.NewOrderedMap(
			[]string{"Tricky", "Vint"},
			map[string]any{
				"Tricky": tricky,
				"Vint":   vnt,
			})
		checkAllAttrs(t, "<TestVariableLength>", got, expAttrs)
	}
}

func TestUnlimitedEmpty(t *testing.T) {
	fileName := "testempty" // base filename without extension
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
	empty := keyValList{
		{"a", "int", api.Variable{
			Values:     []int32{},
			Dimensions: []string{"u"},
			Attributes: nilMap,
		}},
		{"b", "opaque5", api.Variable{
			Values:     []opaque{},
			Dimensions: []string{"u"},
			Attributes: nilMap,
		}},
		{"c", "alltypes", api.Variable{
			Values:     []compound{},
			Dimensions: []string{"u"},
			Attributes: nilMap,
		}},
		{"d", "alltypes", api.Variable{
			Values:     [][]compound{},
			Dimensions: []string{"u", "u"},
			Attributes: nilMap,
		}},
		{"e", "alltypes", api.Variable{
			Values:     [][][]compound{},
			Dimensions: []string{"u", "u", "u"},
			Attributes: nilMap,
		}},
		{"f", "opaque5", api.Variable{
			Values:     [][]opaque{},
			Dimensions: []string{"u", "u"},
			Attributes: nilMap,
		}},
		{"g", "opaque5", api.Variable{
			Values:     [][][]opaque{},
			Dimensions: []string{"u", "u", "u"},
			Attributes: nilMap,
		}},
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
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	unlims := keyValList{
		{"i8x1", "byte", api.Variable{
			Values:     []int8{12, 56},
			Dimensions: []string{"d1"},
			Attributes: nilMap,
		}},
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
	defer os.Remove(genName)
	filtName := ncFilter(t, fileName, filters, extension)
	if filtName == "" {
		t.Error(errorNcFilter)
		return
	}
	defer os.Remove(filtName)
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
	// Usually, FLET is last, but it can appear anywhere.
	// If GZIP is present, SHUF usually appears before it.
	testFilters(t, []string{"SHUF", "FLET"}, "-sf")
	testFilters(t, []string{"SHUF", "GZIP=5"}, "-sg5")
	testFilters(t, []string{"GZIP=5", "FLET"}, "-g5f")
	testFilters(t, []string{"SHUF", "GZIP=5", "FLET"}, "-sg5f")

	// Test Fletcher32 in a non-standard position (Issue 16)
	testFilters(t, []string{"FLET", "SHUF", "GZIP=5"}, "-fsg5")
}

func TestBadMagic(t *testing.T) {
	fileName := "testdata/badmagic" // base filename without extension
	_, err := Open(fileName)
	if err != ErrBadMagic {
		t.Error("bad magic error not seen", err)
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
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(genName)
	defer nc.Close()
	unlims := keyValList{
		{"i16x1", "short", api.Variable{
			Values:     []int16{9876, 5432, 7734},
			Dimensions: []string{"d1"},
			Attributes: nilMap,
		}},
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
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(genName)
	defer nc.Close()
	opaque := keyValList{
		{"v", "opaque5", api.Variable{
			Values: []opaque{
				{0xde, 0xad, 0xbe, 0xef, 0x01},
				{0xde, 0xad, 0xbe, 0xef, 0x02},
				{0xde, 0xad, 0xbe, 0xef, 0x03},
				{0xde, 0xad, 0xbe, 0xef, 0x04},
				{0xde, 0xad, 0xbe, 0xef, 0x05},
			},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
	}
	checkAll(t, nc, opaque)
}

func TestOpaqueCasted(t *testing.T) {
	fileName := "testopaque" // base filename without extension
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
	type opaque5 [5]byte
	var proto opaque5
	h5 := nc.(*HDF5)
	h5.register("opaque5", proto)
	opaque := keyValList{
		{"v", "opaque5", api.Variable{
			Values: []opaque5{
				{0xde, 0xad, 0xbe, 0xef, 0x01},
				{0xde, 0xad, 0xbe, 0xef, 0x02},
				{0xde, 0xad, 0xbe, 0xef, 0x03},
				{0xde, 0xad, 0xbe, 0xef, 0x04},
				{0xde, 0xad, 0xbe, 0xef, 0x05},
			},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
	}
	checkAll(t, nc, opaque)
}

func TestOpaqueBadCasted(t *testing.T) {
	fileName := "testopaque" // base filename without extension
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
	type opaque5 [2]byte // supposed to be 5 bytes. See if that gets detected.
	var proto opaque5
	h5 := nc.(*HDF5)
	h5.register("opaque5", proto)
	opaque := keyValList{
		{"v", "opaque5", api.Variable{
			Values: []opaque{ // not opaque5
				{0xde, 0xad, 0xbe, 0xef, 0x01},
				{0xde, 0xad, 0xbe, 0xef, 0x02},
				{0xde, 0xad, 0xbe, 0xef, 0x03},
				{0xde, 0xad, 0xbe, 0xef, 0x04},
				{0xde, 0xad, 0xbe, 0xef, 0x05},
			},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
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
	defer os.Remove(genName)
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	enum := keyValList{
		{"c", "color", api.Variable{
			Values: enumerated{
				values:     []int8{0, 1, 2, 3, 4, 5},
				names:      []string{"RED", "YELLOW", "GREEN", "CYAN", "BLUE", "MAGENTA"},
				memberVals: []any{int8(0), int8(1), int8(2), int8(3), int8(4), int8(5)},
			},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
		{"j", "junk", api.Variable{
			Values: enumerated{
				values:     []int64{1, 2, 3, 4, 5, 6},
				names:      []string{"FIRST", "SECOND", "THIRD", "FOURTH", "FIFTH", "SIXTH"},
				memberVals: []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
			},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
		{"nodim", "color", api.Variable{
			Values: enumerated{
				values:     int8(2),
				names:      []string{"RED", "YELLOW", "GREEN", "CYAN", "BLUE", "MAGENTA"},
				memberVals: []any{int8(0), int8(1), int8(2), int8(3), int8(4), int8(5)},
			},
			Dimensions: nil,
			Attributes: nilMap,
		}},
		{"c2", "color2", api.Variable{
			Values: enumerated{
				values:     uint16(0),
				names:      []string{"BLACK", "WHITE"},
				memberVals: []any{uint16(0), uint16(1)},
			},
			Dimensions: nil,
			Attributes: nilMap,
		}},
		{"j2", "junk2", api.Variable{
			Values: enumerated{
				values:     int32(7),
				names:      []string{"SEVENTH", "EIGHTH"},
				memberVals: []any{int32(7), int32(8)},
			},
			Dimensions: nil,
			Attributes: nilMap,
		}},
	}
	checkAll(t, nc, enum)
}

func TestEnumCasted(t *testing.T) {
	fileName := "testenum" // base filename without extension
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
	h5 := nc.(*HDF5)
	type color int8
	var zc color
	type junk int64
	var zj junk
	type color2 uint16
	var zc2 color2
	type junk2 int32
	var zj2 junk2
	h5.register("color", zc)
	h5.register("junk", zj)
	h5.register("color2", zc2)
	h5.register("junk2", zj2)
	defer nc.Close()
	enum := keyValList{
		{"c", "color", api.Variable{
			Values:     []color{0, 1, 2, 3, 4, 5},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
		{"j", "junk", api.Variable{
			Values:     []junk{1, 2, 3, 4, 5, 6},
			Dimensions: []string{"dim"},
			Attributes: nilMap,
		}},
		{"nodim", "color", api.Variable{
			Values:     color(2),
			Dimensions: nil,
			Attributes: nilMap,
		}},
		{"c2", "color2", api.Variable{
			Values:     color2(0),
			Dimensions: nil,
			Attributes: nilMap,
		}},
		{"j2", "junk2", api.Variable{
			Values:     junk2(7),
			Dimensions: nil,
			Attributes: nilMap,
		}},
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
	defer os.Remove(genName)
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

func (kl keyValList) check(t *testing.T, name string, baseType string,
	val api.Variable, doAttrs bool) bool {
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
		t.Logf("types name=%s got type=%T exp type=%T", name, val.Values, kv.val.Values)
		return false
	}
	if doAttrs {
		checkAllAttrs(t, name, val.Attributes, kv.val.Attributes)
	}
	if !reflect.DeepEqual(val.Dimensions, kv.val.Dimensions) &&
		!(len(val.Dimensions) == 0 && len(kv.val.Dimensions) == 0) {
		t.Log("dims", name, val.Dimensions, kv.val.Dimensions)
		return false
	}
	if kv.baseType != baseType {
		t.Logf("Type mismatch got=%#v exp=%#v", baseType, kv.baseType)
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
	types := nc.ListTypes()
	for _, typeName := range types {
		val, err := nc.GetVariable(typeName)
		if err == nil {
			t.Errorf("types are not variables got=(%s)", val)
		}
	}
	vars := nc.ListVariables()
	if len(vars) != len(values) {
		t.Error("mismatch", len(vars), len(values), vars, values)
		return
	}
	for _, name := range vars {
		getter, err := nc.GetVarGetter(name)
		if err != nil {
			t.Error(err)
			return
		}
		val, err := getter.Values()
		if err != nil {
			t.Error(err)
			return
		}
		ty := getter.Type()
		attr := getter.Attributes()
		v := api.Variable{
			Values:     val,
			Dimensions: getter.Dimensions(),
			Attributes: attr,
		}
		if !values.check(t, name, ty, v, hasAttr) {
			t.Error("mismatch")
			return
		}
	}
}

func checkAllNoAttr(t *testing.T, nc api.Group, values keyValList) {
	t.Helper()
	checkAllAttrOption(t, nc, values, dontDoAttrs)
}

func checkAll(t *testing.T, nc api.Group, values keyValList) {
	t.Helper()
	checkAllAttrOption(t, nc, values, doAttrs)
	h5, _ := nc.(*HDF5)
	for _, v := range values {
		_ = h5.findType(v.name)
	}
}

func TestDimensions(t *testing.T) {
	fileName := "testunlimited" // base filename without extension
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

func TestListDimensions(t *testing.T) {
	genName := ncGen(t, "testdimensions")
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
	expected := []string{"valid_time", "pressure_level", "latitude", "longitude"}
	if !slices.Equal(dims, expected) {
		t.Errorf("ListDimension(%q): Got %v, want %v\n", genName, dims, expected)
	}
}
