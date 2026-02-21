package cdf

// Tests for makeFillValueReader:
//   - TestCDFFillValueDefaults: default fill values (no user _FillValue), all types.
//   - TestCDFFillValueUserSupplied: user-supplied _FillValue overrides the default.
//   - TestCDFFillValueRoundTrip: end-to-end via ncgen — fill values survive a write/read cycle.

import (
	"bytes"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/batchatco/go-native-netcdf/internal"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

func emptyAttrs(t *testing.T) *util.OrderedMap {
	t.Helper()
	m, err := util.NewOrderedMap(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func attrsWithFillValue(t *testing.T, fv any) *util.OrderedMap {
	t.Helper()
	m, err := util.NewOrderedMap([]string{"_FillValue"}, map[string]any{"_FillValue": fv})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readFillBytes(t *testing.T, v variable, n int) []byte {
	t.Helper()
	r := makeFillValueReader(v, bytes.NewReader(nil))
	got := make([]byte, n)
	_, err := io.ReadFull(r, got)
	if err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	return got
}

func TestCDFFillValueDefaults(t *testing.T) {
	cases := []struct {
		name  string
		vtype uint32
		want  []byte
	}{
		// big-endian byte patterns for each type's default fill value
		{"byte", typeByte, []byte{0x81}},
		{"ubyte", typeUByte, []byte{0xff}},
		{"short", typeShort, []byte{0x80, 0x01}},
		{"ushort", typeUShort, []byte{0xff, 0xff}},
		{"int", typeInt, []byte{0x80, 0x00, 0x00, 0x01}},
		{"uint", typeUInt, []byte{0xff, 0xff, 0xff, 0xff}},
		{"int64", typeInt64, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}},
		{"uint64", typeUInt64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{"float", typeFloat, func() []byte {
			f := math.Float32frombits(internal.FillFloat)
			return makeFillValueFloat(f)
		}()},
		{"double", typeDouble, func() []byte {
			d := math.Float64frombits(internal.FillDouble)
			return makeFillValueDouble(d)
		}()},
		{"char", typeChar, []byte{0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := variable{vType: tc.vtype, attrs: emptyAttrs(t)}
			got := readFillBytes(t, v, len(tc.want))
			if !bytes.Equal(got, tc.want) {
				t.Errorf("default fill for %s: got %x, want %x", tc.name, got, tc.want)
			}
		})
	}
}

func TestCDFFillValueUserSupplied(t *testing.T) {
	cases := []struct {
		name  string
		vtype uint32
		fv    any
		want  []byte
	}{
		{"byte", typeByte, int8(-50), []byte{0xce}},
		{"ubyte", typeUByte, uint8(200), []byte{0xc8}},
		{"short", typeShort, int16(-1000), []byte{0xfc, 0x18}},
		{"ushort", typeUShort, uint16(60000), []byte{0xea, 0x60}},
		{"int", typeInt, int32(-1000000), []byte{0xff, 0xf0, 0xbd, 0xc0}},
		{"uint", typeUInt, uint32(3000000000), []byte{0xb2, 0xd0, 0x5e, 0x00}},
		{"int64", typeInt64, int64(-1), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"uint64", typeUInt64, uint64(1), []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{"float", typeFloat, float32(1.5), makeFillValueFloat(1.5)},
		{"double", typeDouble, float64(1.5), makeFillValueDouble(1.5)},
		{"char", typeChar, byte('Z'), []byte{'Z'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := variable{vType: tc.vtype, attrs: attrsWithFillValue(t, tc.fv)}
			got := readFillBytes(t, v, len(tc.want))
			if !bytes.Equal(got, tc.want) {
				t.Errorf("user fill for %s: got %x, want %x", tc.name, got, tc.want)
			}
		})
	}
}

func TestCDFFillValueRoundTrip(t *testing.T) {
	// int64 is omitted: ncgen -k nc5 writes int64 fill values as int32 (ncgen bug)
	cdl := `
netcdf cdf5fill {
dimensions:
	dim = 1 ;
variables:
	byte b(dim) ;
	ubyte ub(dim) ;
	short s(dim) ;
	ushort us(dim) ;
	int i(dim) ;
	uint ui(dim) ;
	uint64 ui64(dim) ;
	float f(dim) ;
	double d(dim) ;
data:
	b = _ ;
	ub = _ ;
	s = _ ;
	us = _ ;
	i = _ ;
	ui = _ ;
	ui64 = _ ;
	f = _ ;
	d = _ ;
}
`
	tmpDir := t.TempDir()
	cdlPath := filepath.Join(tmpDir, "cdf5fill.cdl")
	ncPath := filepath.Join(tmpDir, "cdf5fill.nc")

	if err := os.WriteFile(cdlPath, []byte(cdl), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("ncgen", "-b", "-k", "nc5", "-o", ncPath, cdlPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("ncgen not available or failed: %v\n%s", err, out)
	}

	nc, err := Open(ncPath)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	expected := []struct {
		name string
		want any
	}{
		{"b", []int8{int8(internal.FillByte)}},
		{"ub", []uint8{internal.FillUByte}},
		{"s", []int16{int16(internal.FillShort)}},
		{"us", []uint16{internal.FillUShort}},
		{"i", []int32{int32(internal.FillInt)}},
		{"ui", []uint32{internal.FillUInt}},
		// i64 skipped: ncgen -k nc5 writes int64 fill value as int32 (ncgen bug)
		{"ui64", []uint64{internal.FillUInt64}},
		{"f", []float32{math.Float32frombits(internal.FillFloat)}},
		{"d", []float64{math.Float64frombits(internal.FillDouble)}},
	}
	for _, tc := range expected {
		t.Run(tc.name, func(t *testing.T) {
			v, err := nc.GetVariable(tc.name)
			if err != nil {
				t.Fatalf("GetVariable(%s): %v", tc.name, err)
			}
			match := reflect.DeepEqual(v.Values, tc.want)
			if !match {
				if got, ok := v.Values.([]float32); ok {
					want := tc.want.([]float32)
					match = len(got) == len(want) &&
						math.Float32bits(got[0]) == math.Float32bits(want[0])
				} else if got, ok := v.Values.([]float64); ok {
					want := tc.want.([]float64)
					match = len(got) == len(want) &&
						math.Float64bits(got[0]) == math.Float64bits(want[0])
				}
			}
			if !match {
				t.Errorf("variable %s: got %v, want %v", tc.name, v.Values, tc.want)
			}
		})
	}
}
