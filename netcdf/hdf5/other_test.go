// Test HDF5 specific things that aren't part of NetCDF classic

// TODO: convert netcdf data files to our own tests. Figure out how to make
// HDF5 ascii to binary.
// TODO: make test of type time, bitfield, opaque, reference.
package hdf5

import (
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

const (
	ncDir = "/home/bat/src/netcdf/"
	h5Dir = "/home/bat/src/hdf5/"
)

var logThisFile = map[string]bool{
	//"single_latest.h5": true,
	//"h5clear_mdc_image.h5": true,
	// basenames only, not full paths
	//"tarray4.h5": true,
}

// Skip these files because they tickle bugs (or are bugs
// themselves).
var skipFiles = map[string]bool{
	// basenames only, not full paths
	//"tattrreg.h5": true,
}

// Skip getting variables for these files because they are too big
var skipVariables = map[string]bool{
	"h5stat_newgrat.h5": true, // too many groups, test times out
}

// Just get some variables from these large files
var someVariables = map[string]bool{
	"tbigdims.h5":    true,
	"tst_chunks.nc":  true, // blows up ncdump
	"tst_chunks2.nc": true, // blows up ncdump
}

func getFiles(t *testing.T, pathName string, suffix string) map[string]bool {
	names := make(map[string]bool)
	st, err := os.Stat(pathName)
	if err != nil {
		t.Error(pathName, err)
		return nil
	}
	if !st.IsDir() {
		names[pathName] = true
		return names
	}
	files, err := ioutil.ReadDir(pathName)
	if err != nil {
		t.Log("Error opening", pathName, err)
		return nil
	}
	if !strings.HasSuffix(pathName, "/") {
		pathName = pathName + "/"
	}
	for _, file := range files {
		fullName := pathName + file.Name()
		if file.IsDir() {
			more := getFiles(t, fullName, suffix)
			for m := range more {
				names[m] = true
			}
		} else if strings.HasSuffix(file.Name(), suffix) {
			if isH5File(fullName) {
				names[fullName] = true
			}
		}
	}
	return names
}

func isH5File(fileName string) bool {
	file, err := os.Open(fileName)
	if err != nil {
		return false
	}
	defer file.Close()

	var b [1]byte
	n, err := file.Read(b[:])
	if n == 0 || err != nil {
		return false
	}
	return b[0] == 0x89
}

func openFile(file string) (nc api.Group, err error) {
	return Open(file)
}

func command(t *testing.T, cmdString []string, fname string) (ok bool) {
	command := cmdString[0]
	cmd := exec.Command(command, append(cmdString[1:], fname)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Log(command, "error (exec)", err)
		return false
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Log(command, "error (exec)", err)
		return false
	}
	err = cmd.Start()
	ok = false
	defer func() {
		err := cmd.Wait()
		if err != nil {
			t.Log(command, "error (wait)", err)
			ok = false
			return
		}
		ok = true
	}()
	if err != nil {
		t.Log(command, "error (start)", err)
		return false
	}
	for {
		var b [1024 * 1024]byte
		_, err := stdout.Read(b[:])
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		t.Log(err)
		return false
	}
	errText, r := ioutil.ReadAll(stderr)
	if r == nil {
		if len(errText) > 0 {
			if len(errText) > 80 {
				errText = errText[:80]
			}
			t.Log(string(errText))
		}
	} else {
		t.Log("readall err", r)
	}
	return true
}

func hasAlreadyFailed(fname string) bool {
	_, err := os.Stat(fname + ".fails")
	return err == nil
}

func recordFail(fname string) {
	f, err := os.Create(fname + ".fails")
	if err == nil {
		f.Close()
	}
}

func ncDump(t *testing.T, fname string, headersOnly bool) (fails bool) {
	if hasAlreadyFailed(fname) {
		t.Log("Cached failure for", fname)
		return false
	}
	var ret bool
	if headersOnly {
		ret = command(t, []string{"ncdump", "-h", "-s"}, fname)
	} else {
		ret = command(t, []string{"ncdump", "-s"}, fname)
	}
	if !ret {
		recordFail(fname)
	}
	return ret
}

func ncCopy(t *testing.T, fname string) string {
	base := path.Base(fname)
	dst := "/tmp/" + base
	if !command(t, []string{"nccopy", "-k", "nc4", fname}, dst) {
		os.Remove(dst)
		return ""
	}
	return dst
}

func h5Dump(t *testing.T, fname string) (fails bool) {
	if hasAlreadyFailed(fname) {
		t.Log("Cached failure for", fname)
		return false
	}
	ret := command(t, []string{"h5dump", "-H", "-p"}, fname)
	if !ret {
		recordFail(fname)
	}
	return ret
}

func convertCommon(t *testing.T, dir string, suffix string) {
	t.Helper()
	files := getFiles(t, dir, suffix)
	for file := range files {
		func(file string) {
			baseName := path.Base(file)
			if skipFiles[baseName] {
				return
			}
			novars := skipVariables[baseName]
			somevars := someVariables[baseName]
			errorIsOk := false
			if strings.Contains(file, ".h5") {
				defer setNonStandard(setNonStandard(true))
				defer setBitfields(setBitfields(true))
				defer setReferences(setReferences(true))
				if !h5Dump(t, file) {
					t.Log("h5dump has errors, errors not fatal", file)
					errorIsOk = true
				}
			}
			if strings.Contains(file, ".nc") {
				if !ncDump(t, file, somevars) {
					t.Log("ncdump has errors, errors not fatal", file)
					errorIsOk = true
				}
				if false {
					fn := ncCopy(t, file)
					if fn == "" {
						errorIsOk = true
						t.Log("nccopy failed")
					} else {
						file = fn
						defer func() {
							os.Remove(fn)
						}()
					}
				}
			}
			logit := func(s ...string) {
				if errorIsOk {
					t.Log("Error OK:", s)
				} else {
					t.Error("Error not OK, FAIL:", s)
				}
			}
			if logThisFile[baseName] {
				t.Log("logging on", baseName)
				defer SetLogLevel(SetLogLevel(3))
				// defer thrower.SetCatching(thrower.SetCatching(thrower.DontCatch))
			}
			t.Log("TEST:", file)
			nc, err := openFile(file)
			switch err {
			case nil:
				// success
				if errorIsOk {
					t.Log("Unexpected success", file)
				}
			case ErrVersion, ErrLinkType, ErrLayout, ErrOffsetSize, ErrSuperblock,
				ErrUnsupportedFilter, ErrBitfield, ErrVirtualStorage, ErrExternal,
				ErrFloatingPoint, ErrFixedPoint, ErrReference:
				t.Log("Expected error", file, err)
				return
			default:
				if errorIsOk {
					t.Log("Unexpected error, but OK", file, err)
				} else {
					t.Error("Unexpected error, FAIL:", file, err)
				}
				return
			}
			var dumpGroup func(nc api.Group)
			dumpGroup = func(nc api.Group) {
				defer nc.Close()
				switch {
				// Some variables, but nothing else
				case somevars:
					for _, val := range nc.ListVariables() {
						vg, err := nc.GetVarGetter(val)
						if err != nil {
							logit("getvargetter", file, val, err.Error())
							continue
						}
						chunkSize := int64(1)
						num := vg.Len()
						if num > 100 {
							num = 100
						}
						for i := int64(0); i < num; i++ {
							_, err := vg.GetSlice(int64(i)*chunkSize, int64(i+1)*chunkSize)
							if err != nil {
								logit("getslice", file, val, err.Error())
								break
							}
						}
					}
				case !novars:
					for _, val := range nc.ListVariables() {
						vr, err := nc.GetVariable(val)
						if err != nil {
							logit("getvar", file, val, err.Error())
							break
						}
						for _, k := range vr.Attributes.Keys() {
							a, ok := vr.Attributes.Get(k)
							// t.Logf("Attribute: %v value: %#v", k, a)
							_ = a
							if !ok {
								logit("get attr failed", file, k)
							}
						}
					}

					groups := nc.ListSubgroups()
					for _, group := range groups {
						nc2, err := nc.GetGroup(group)
						if err != nil {
							logit("get group", file, group, err.Error())
							break
						}
						// t.Log("dump group", group)
						dumpGroup(nc2)
					}
					for _, k := range nc.Attributes().Keys() {
						a, ok := nc.Attributes().Get(k)
						if !ok {
							logit("global getattr", file, k, err.Error())
						} else {
							// t.Logf("Global attribute: %v value: %#v", k, a)
							_ = a
						}
					}
				}
			}
			dumpGroup(nc)
		}(file)
	}
}

func quickTests(t *testing.T) bool {
	quick := os.Getenv("QUICK_TESTS")
	if quick == "true" {
		t.Log("Skipping because QUICK_TESTS env var is set")
		return true
	}
	t.Log("QUICK_TESTS=", quick)
	return false
}

func TestConvert(t *testing.T) {
	// defer thrower.SetCatching(thrower.SetCatching(thrower.DontCatch))
	if quickTests(t) {
		return
	}
	// defer SetLogLevel(SetLogLevel(3))
	t.Log("TestConvert start")
	// 74.7 -> 83.8% coverage
	convertCommon(t, ncDir, ".nc")
}

func TestHDF5(t *testing.T) {
	if quickTests(t) {
		return
	}
	t.Log("TestHDF5 start")
	// 74.7 -> 75.9% coverage
	convertCommon(t, ncDir, ".h5")
	// 74.7 -> 83.9% coverage
	convertCommon(t, h5Dir, ".h5")
}

func TestOneFile(t *testing.T) {
	doTest := false
	if !doTest {
		return
	}
	// defer SetLogLevel(SetLogLevel(3))
	defer setNonStandard(setNonStandard(true))
	// convertCommon(t, "/home/bat/src/netcdf/nc_test4/tst_vars3.nc", "")
	filename := "/home/bat/src/hdf5/tools/test/h5stat/testfiles/h5stat_newgrat.h5"
	defer setSuperblockV3(setSuperblockV3(true))
	defer setSBExtension(setSBExtension(true))
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()
	for _, val := range nc.ListVariables() {
		vg, err := nc.GetVarGetter(val)
		if err != nil {
			t.Error("getvargetter can't get var", val, err.Error())
			continue
		}
		chunkSize := int64(1)
		num := vg.Len()
		if num > 100 {
			num = 100
		}
		for i := int64(0); i < num; i++ {
			_, err := vg.GetSlice(int64(i)*chunkSize, int64(i+1)*chunkSize)
			if err != nil {
				t.Error("getslice", val, i, err.Error())
				break
			}
		}
	}
}

// TestDisabled
func TestDisabled(t *testing.T) {
	if quickTests(t) {
		return
	}
	defer setSBExtension(setSBExtension(true))
	defer setParseHeapDirectBlock(setParseHeapDirectBlock(true))
	defer setSuperblockV3(setSuperblockV3(true))

	t.Run("TestHDF5", TestHDF5)
	t.Run("TestConvert", TestConvert)
}

func TestFunky(t *testing.T) {
	defer setBitfields(setBitfields(true))
	defer setReferences(setReferences(true))

	filename := "/home/bat/src/hdf5/tools/test/h5repack/testfiles/h5repack_attr.h5"
	basename := path.Base(filename)
	t.Log("Further test:", basename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()

	exp, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{"string", "bitfield", "opaque", "compound", "reference", "enum", "vlen", "array", "integer", "float", "string2D", "bitfield2D", "opaque2D", "compound2D", "reference2D", "enum2D", "vlen2D", "array2D", "integer2D", "float2D", "string3D", "bitfield3D", "opaque3D", "compound3D", "reference3D", "enum3D", "vlen3D", "array3D", "integer3D", "float3D"},
		map[string]interface{}{
			"array": [][]int32{{1, 2, 3}, {4, 5, 6}},
			"array2D": [][][]int32{
				{{1, 2, 3}, {4, 5, 6}},
				{{7, 8, 9}, {10, 11, 12}},
				{{13, 14, 15}, {16, 17, 18}},
			},
			"array3D": [][][][]int32{
				{
					{{1, 2, 3}, {4, 5, 6}},
					{{7, 8, 9}, {10, 11, 12}},
					{{13, 14, 15}, {16, 17, 18}},
				},
				{
					{{19, 20, 21}, {22, 23, 24}},
					{{25, 26, 27}, {28, 29, 30}},
					{{31, 32, 33}, {34, 35, 36}},
				},
				{
					{{37, 38, 39}, {40, 41, 42}},
					{{43, 44, 45}, {46, 47, 48}},
					{{49, 50, 51}, {52, 53, 54}},
				},
				{
					{{55, 56, 57}, {58, 59, 60}},
					{{61, 62, 63}, {64, 65, 66}},
					{{67, 68, 69}, {70, 71, 72}},
				},
			},
			"bitfield":   []byte{0x1, 0x2},
			"bitfield2D": [][]byte{{0x1, 0x2}, {0x3, 0x4}, {0x5, 0x6}},
			"bitfield3D": [][][]byte{
				{{0x1, 0x2}, {0x3, 0x4}, {0x5, 0x6}},
				{{0x7, 0x8}, {0x9, 0xa}, {0xb, 0xc}},
				{{0xd, 0xe}, {0xf, 0x10}, {0x11, 0x12}},
				{{0x13, 0x14}, {0x15, 0x16}, {0x17, 0x18}},
			},
			"compound": []compound{
				{{Name: "a", Val: int8(1)}, {Name: "b", Val: float64(2)}},
				{{Name: "a", Val: int8(3)}, {Name: "b", Val: float64(4)}},
			},
			"compound2D": [][]compound{
				{
					{{Name: "a", Val: int8(1)}, {Name: "b", Val: float64(2)}},
					{{Name: "a", Val: int8(3)}, {Name: "b", Val: float64(4)}},
				},
				{
					{{Name: "a", Val: int8(5)}, {Name: "b", Val: float64(6)}},
					{{Name: "a", Val: int8(7)}, {Name: "b", Val: float64(8)}},
				},
				{
					{{Name: "a", Val: int8(9)}, {Name: "b", Val: float64(10)}},
					{{Name: "a", Val: int8(11)}, {Name: "b", Val: float64(12)}},
				},
			},
			"compound3D": [][][]compound{
				{
					{
						{{Name: "a", Val: int8(1)}, {Name: "b", Val: float64(2)}},
						{{Name: "a", Val: int8(3)}, {Name: "b", Val: float64(4)}},
					},
					{
						{{Name: "a", Val: int8(5)}, {Name: "b", Val: float64(6)}},
						{{Name: "a", Val: int8(7)}, {Name: "b", Val: float64(8)}},
					},
					{
						{{Name: "a", Val: int8(9)}, {Name: "b", Val: float64(10)}},
						{{Name: "a", Val: int8(11)}, {Name: "b", Val: float64(12)}},
					},
				},
				{
					{
						{{Name: "a", Val: int8(13)}, {Name: "b", Val: float64(14)}},
						{{Name: "a", Val: int8(15)}, {Name: "b", Val: float64(16)}},
					},
					{
						{{Name: "a", Val: int8(17)}, {Name: "b", Val: float64(18)}},
						{{Name: "a", Val: int8(19)}, {Name: "b", Val: float64(20)}},
					},
					{
						{{Name: "a", Val: int8(21)}, {Name: "b", Val: float64(22)}},
						{{Name: "a", Val: int8(23)}, {Name: "b", Val: float64(24)}},
					},
				},
				{
					{
						{{Name: "a", Val: int8(25)}, {Name: "b", Val: float64(26)}},
						{{Name: "a", Val: int8(27)}, {Name: "b", Val: float64(28)}},
					},
					{
						{{Name: "a", Val: int8(29)}, {Name: "b", Val: float64(30)}},
						{{Name: "a", Val: int8(31)}, {Name: "b", Val: float64(32)}},
					},
					{
						{{Name: "a", Val: int8(33)}, {Name: "b", Val: float64(34)}},
						{{Name: "a", Val: int8(35)}, {Name: "b", Val: float64(36)}},
					},
				},
				{
					{
						{{Name: "a", Val: int8(37)}, {Name: "b", Val: float64(38)}},
						{{Name: "a", Val: int8(39)}, {Name: "b", Val: float64(40)}},
					},
					{
						{{Name: "a", Val: int8(41)}, {Name: "b", Val: float64(42)}},
						{{Name: "a", Val: int8(43)}, {Name: "b", Val: float64(44)}},
					},
					{
						{{Name: "a", Val: int8(45)}, {Name: "b", Val: float64(46)}},
						{{Name: "a", Val: int8(47)}, {Name: "b", Val: float64(48)}},
					},
				},
			},

			"enum": enumerated{values: []int32{0, 0}},

			"enum2D": enumerated{values: [][]int32{{0, 0}, {0, 0}, {0, 0}}},

			"enum3D": enumerated{values: [][][]int32{
				{{1, 1}, {1, 1}, {1, 1}},
				{{1, 1}, {1, 1}, {1, 1}},
				{{1, 1}, {1, 1}, {1, 1}},
				{{1, 1}, {1, 1}, {1, 1}},
			}},
			"float": []float32{1, 2},

			"float2D": [][]float32{{1, 2}, {3, 4}, {5, 6}},

			"float3D": [][][]float32{
				{{1, 2}, {3, 4}, {5, 6}},
				{{7, 8}, {9, 10}, {11, 12}},
				{{13, 14}, {15, 16}, {17, 18}},
				{{19, 20}, {21, 22}, {23, 24}},
			},

			"integer": []int32{1, 2},

			"integer2D": [][]int32{{1, 2}, {3, 4}, {5, 6}},

			"integer3D": [][][]int32{
				{{1, 2}, {3, 4}, {5, 6}},
				{{7, 8}, {9, 10}, {11, 12}},
				{{13, 14}, {15, 16}, {17, 18}},
				{{19, 20}, {21, 22}, {23, 24}},
			},
			"opaque":   []opaque{{0x1}, {0x2}},
			"opaque2D": [][]opaque{{{0x1}, {0x2}}, {{0x3}, {0x4}}, {{0x5}, {0x6}}},
			"opaque3D": [][][]opaque{
				{{{0x1}, {0x2}}, {{0x3}, {0x4}}, {{0x5}, {0x6}}},
				{{{0x7}, {0x8}}, {{0x9}, {0xa}}, {{0xb}, {0xc}}},
				{{{0xd}, {0xe}}, {{0xf}, {0x10}}, {{0x11}, {0x12}}},
				{{{0x13}, {0x14}}, {{0x15}, {0x16}}, {{0x17}, {0x18}}},
			},

			"reference":   []uint64{800, 800},
			"reference2D": [][]uint64{{800, 800}, {800, 800}, {800, 800}},
			"reference3D": [][][]uint64{{{800, 800}, {800, 800}, {800, 800}}, {{800, 800}, {800, 800}, {800, 800}}, {{800, 800}, {800, 800}, {800, 800}}, {{800, 800}, {800, 800}, {800, 800}}},

			"string": "ab",

			"string2D": "ab",
			"string3D": "ab",

			"vlen": [][]int32{
				{1},
				{2, 3},
			},

			"vlen2D": [][][]int32{
				{{0}, {1}},
				{{2, 3}, {4, 5}},
				{{6, 7, 8}, {9, 10, 11}},
			},

			"vlen3D": [][][][]int32{
				{
					{{0}, {1}},
					{{2}, {3}},
					{{4}, {5}},
				},
				{
					{{6, 7}, {8, 9}},
					{{10, 11}, {12, 13}},
					{{14, 15}, {16, 17}},
				},
				{
					{{18, 19, 20}, {21, 22, 23}},
					{{24, 25, 26}, {27, 28, 29}},
					{{30, 31, 32}, {33, 34, 35}},
				},

				{
					{{36, 37, 38, 39}, {40, 41, 42, 43}},
					{{44, 45, 46, 47}, {48, 49, 50, 51}},
					{{52, 53, 54, 55}, {56, 57, 58, 59}},
				},
			},
		})
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	h5, _ := nc.(*HDF5)
	for _, key := range exp.Keys() {
		_ = h5.findGlobalAttrType(key)
		// t.Logf("attribute type for %s = %s.", key, ty)
	}
	checkAllAttrs(t, "<testfunky>", nc.Attributes(), exp)
}

func TestFunky2(t *testing.T) {
	filename := "/home/bat/src/netcdf/ncdump/tst_comp2.nc"
	basename := path.Base(filename)
	t.Log("Further test:", basename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()

	attr, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{"_FillValue"},
		map[string]interface{}{"_FillValue": compound{
			{Name: "day", Val: "?"},
			{Name: "mnth", Val: "---"},
			{Name: "vect", Val: []int16{-1, -2, -3}},
			{Name: "matr", Val: [][]float32{{-4, -5, -6}, {-7, -8, -9}}},
		}})
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	obs := keyValList{
		{"obs", "vecmat_t", api.Variable{
			Values: []compound{
				{
					{Name: "day", Val: "S"},
					{Name: "mnth", Val: "jan"},
					{Name: "vect", Val: []int16{1, 2, 3}},
					{Name: "matr", Val: [][]float32{{4, 5, 6}, {7, 8, 9}}},
				},
				{
					{Name: "day", Val: "M"},
					{Name: "mnth", Val: "feb"},
					{Name: "vect", Val: []int16{11, 12, 13}},
					{Name: "matr", Val: [][]float32{{4.25, 5.25, 6.25}, {7.25, 8.25, 9.25}}},
				},
				{
					{Name: "day", Val: "T"},
					{Name: "mnth", Val: "mar"},
					{Name: "vect", Val: []int16{21, 22, 23}},
					{Name: "matr", Val: [][]float32{{4.5, 5.5, 6.5}, {7.5, 8.5, 9.5}}},
				},
			},
			Dimensions: []string{"n"},
			Attributes: attr,
		}},
	}
	checkAll(t, nc, obs)
}

func TestFunky9(t *testing.T) {
	filename := "/home/bat/src/netcdf/ncdump/ref_tst_compounds4.nc"
	t.Log("Further test:", filename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()
	exp, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{"a1"},
		map[string]interface{}{
			"a1": compound{
				{
					Name: "s1",
					Val: compound{
						{Name: "x", Val: float32(1)},
						{Name: "y", Val: float64(-2)},
					},
				},
			},
		})
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	checkAllAttrs(t, "<testfunky9>", nc.Attributes(), exp)
}

func TestFunky8(t *testing.T) {
	filename := "/home/bat/src/netcdf/nc_test4/tst_empty_vlen_lim.nc"
	t.Log("Further test:", filename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()

	obs := keyValList{
		{"v", "vltest", api.Variable{
			Values: [][]float32{
				{0, 1},
				{0, 1, 2},
				{},
				{},
				{},
			},
			Dimensions: []string{"x"},
			Attributes: nilMap,
		}},
		{"w", "float", api.Variable{
			Values:     []float32{0, 1, 2, 9.96921e+36, 9.96921e+36},
			Dimensions: []string{"x"},
			Attributes: nilMap,
		}},
	}

	checkAll(t, nc, obs)
}

func TestFunky3(t *testing.T) {
	filename := "/home/bat/src/netcdf/nc_test4/tst_vars4.nc"
	basename := path.Base(filename)
	t.Log("Further test:", basename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()

	clair := make([][]int32, 2)
	for i := range clair {
		clair[i] = make([]int32, 8193)
		for j := range clair[i] {
			clair[i][j] = math.MinInt32 + 1
		}
	}
	vals := keyValList{
		{"y", "int", api.Variable{
			Values:     []int32{math.MinInt32 + 1, math.MinInt32 + 1},
			Dimensions: []string{"x"},
			Attributes: nilMap,
		}},
		{"Clair", "int", api.Variable{
			Values:     clair,
			Dimensions: []string{"x", "z"},
			Attributes: nilMap,
		}},
		{"Jamie", "int", api.Variable{
			Values:     int32(0),
			Dimensions: nil,
			Attributes: nilMap,
		}},
	}
	checkAll(t, nc, vals)
}

func TestFunky4(t *testing.T) {
	filename := "/home/bat/src/netcdf/nc_test4/tst_atts1.nc"
	basename := path.Base(filename)
	t.Log("Further test:", basename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()
	exp, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{
			"Speech_to_Jury",
			"Slate_totals_at_Pomeroys_Wine_Bar",
			"Number_of_current_briefs",
			"Ecclesiastical_Court_Appearences",
			"Old_Bailey_Room_Numbers",
			"Average_Nanoseconds_for_Lose_Win_or_Appeal",
			"Equity_Court_Canteen_Charges",
			"brief_no",
			"Orders_from_SWMBO",
			"judges_golf_score",
			"Number_of_drinks_in_career_to_date",
		},
		map[string]interface{}{
			"Speech_to_Jury": "Once more unto the breach, dear friends, once more;\nOr close the wall up with our English dead.\nIn peace there's nothing so becomes a man\nAs modest stillness and humility:\nBut when the blast of war blows in our ears,\nThen imitate the action of the tiger;\nStiffen the sinews, summon up the blood,\nDisguise fair nature with hard-favour'd rage;\nThen lend the eye a terrible aspect;\nLet pry through the portage of the head\nLike the brass cannon; let the brow o'erwhelm it\nAs fearfully as doth a galled rock\nO'erhang and jutty his confounded base,\nSwill'd with the wild and wasteful ocean.\nNow set the teeth and stretch the nostril wide,\nHold hard the breath and bend up every spirit\nTo his full height. On, on, you noblest English.\nWhose blood is fet from fathers of war-proof!\nFathers that, like so many Alexanders,\nHave in these parts from morn till even fought\nAnd sheathed their swords for lack of argument:\nDishonour not your mothers; now attest\nThat those whom you call'd fathers did beget you.\nBe copy now to men of grosser blood,\nAnd teach them how to war. And you, good yeoman,\nWhose limbs were made in England, show us here\nThe mettle of your pasture; let us swear\nThat you are worth your breeding; which I doubt not;\nFor there is none of you so mean and base,\nThat hath not noble lustre in your eyes.\nI see you stand like greyhounds in the slips,\nStraining upon the start. The game's afoot:\nFollow your spirit, and upon this charge\nCry 'God for Harry, England, and Saint George!'",

			"Slate_totals_at_Pomeroys_Wine_Bar":          []int8{-128, 1, 127},
			"Number_of_current_briefs":                   []uint8{0, 128, 255},
			"Ecclesiastical_Court_Appearences":           []int16{-32768, -128, 32767},
			"Old_Bailey_Room_Numbers":                    []int32{-100000, 128, 100000},
			"Average_Nanoseconds_for_Lose_Win_or_Appeal": []float32{0.5, 0.25, 0.125},

			// found
			"Equity_Court_Canteen_Charges":       []float64{0.25, 0.5, 0.125},
			"brief_no":                           []uint16{0, 128, 65535},
			"Orders_from_SWMBO":                  []uint32{0, 128, 4294967295},
			"judges_golf_score":                  []int64{-9223372036854775808, 128, 9223372036854775807},
			"Number_of_drinks_in_career_to_date": []uint64{0, 128, 18446744073709551612},
		})
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	checkAllAttrs(t, "<testfunky4>", nc.Attributes(), exp)
}

func TestFunky5(t *testing.T) {
	filename := "/home/bat/src/netcdf/nc_test/tst_small_netcdf4.nc"
	basename := path.Base(filename)
	t.Log("Further test:", basename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()

	attr, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{
			"a_97", "a_98", "a_99", "a_100", "a_101", "a_102", "a_103", "a_104", "a_105", "a_106",
		},

		map[string]interface{}{
			"a_97": "a", "a_98": "b", "a_99": "c", "a_100": "d",
			"a_101": "e", "a_102": "f", "a_103": "g", "a_104": "h", "a_105": "i", "a_106": "j",
		})
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	exp := keyValList{
		{"Times", "string", api.Variable{
			Values:     "abcdefghij",
			Dimensions: []string{"Time"},
			Attributes: attr,
		}},
		{"var2", "string", api.Variable{
			Values:     "abcdefghij",
			Dimensions: []string{"Time"},
			Attributes: attr,
		}},
	}
	checkAll(t, nc, exp)
}

func TestFunky6(t *testing.T) {
	filename := "/home/bat/src/netcdf/examples/C/simple_nc4.nc"
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()

	nc2, err := nc.GetGroup("grp2")
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	vals := make([][]compound, 6)
	for i := range vals {
		vals[i] = make([]compound, 12)
		v := vals[i]
		for j := range v {
			v[j] = make([]compoundField, 2)
			v[j][0] = compoundField{Name: "i1", Val: int32(42)}
			v[j][1] = compoundField{Name: "i2", Val: int32(-42)}
		}
	}
	grp2 := keyValList{
		{
			"data", "sample_compound_type",
			api.Variable{
				Values:     vals,
				Dimensions: []string{"x", "y"},
				Attributes: nilMap,
			},
		},
	}
	checkAll(t, nc2, grp2)
	v, _ := nc2.GetVariable("data")
	got := v.Values.([][]compound)
	exp := grp2[0].val.Values.([][]compound)
	fieldGot := got[0][0][0]
	fieldExp := exp[0][0][0]
	t.Logf("types %T %T", fieldGot, fieldExp)
}

func TestFunky7(t *testing.T) {
	filename := "/home/bat/src/netcdf/h5_test/tst_h_files2.h5"
	basename := path.Base(filename)
	t.Log("Further test:", basename)
	nc, err := Open(filename)
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	defer nc.Close()
	exp, err := newTypedAttributeMap(nc.(*HDF5),
		[]string{"att_name"},
		map[string]interface{}{
			"att_name": []opaque{
				{
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
				},
				{
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
				},
				{
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
					0x2a, 0x2a, 0x2a, 0x2a, 0x2a,
				},
			},
		})
	if err != nil {
		t.Error("FAIL:", err)
		return
	}
	checkAllAttrs(t, "<testfunky7>", nc.Attributes(), exp)
}

func TestFunky10(t *testing.T) {
	defer setNonStandard(setNonStandard(true))
	filename := "/home/bat/src/hdf5/tools/testfiles/tarray4.h5"
	// filename := "/home/bat/hdf5/new.h5"
	nc, err := Open(filename)
	if err != nil {
		if err != ErrLayout {
			t.Error("Expected layout error, got", err)
		}
		return
	}
	defer nc.Close()

	// this doesn't get executed because we don't support the data layout.
	// keeping it here in case that changes.
	attr, _ := newTypedAttributeMap(nc.(*HDF5), []string{}, map[string]interface{}{})
	typeString := "compound {\n\tint i;\n\tfloat f;\n}(4)"
	exp := keyValList{
		{"Dataset1", typeString, api.Variable{
			Values: [][]compound{
				{
					{{"i", int32(0)}, {"f", float32(0)}},
					{{"i", int32(1)}, {"f", float32(1)}},
					{{"i", int32(2)}, {"f", float32(2)}},
					{{"i", int32(3)}, {"f", float32(3)}},
				},

				{
					{{"i", int32(10)}, {"f", float32(2.5)}},
					{{"i", int32(11)}, {"f", float32(3.5)}},
					{{"i", int32(12)}, {"f", float32(4.5)}},
					{{"i", int32(13)}, {"f", float32(5.5)}},
				},
				{
					{{"i", int32(20)}, {"f", float32(5)}},
					{{"i", int32(21)}, {"f", float32(6)}},
					{{"i", int32(22)}, {"f", float32(7)}},
					{{"i", int32(23)}, {"f", float32(8)}},
				},
				{

					{{"i", int32(30)}, {"f", float32(7.5)}},
					{{"i", int32(31)}, {"f", float32(8.5)}},
					{{"i", int32(32)}, {"f", float32(9.5)}},
					{{"i", int32(33)}, {"f", float32(10.5)}},
				},
			},
			Dimensions: []string{},
			Attributes: attr,
		}},
	}
	checkAll(t, nc, exp)
}
