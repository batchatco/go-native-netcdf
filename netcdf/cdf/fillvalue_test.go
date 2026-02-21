package cdf

// Tests for makeFillValueReader:
//   - TestCDFFillValueDefaults: default fill values (no user _FillValue), all types.
//   - TestCDFFillValueUserSupplied: user-supplied _FillValue overrides the default.

import (
	"bytes"
	"io"
	"math"
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
