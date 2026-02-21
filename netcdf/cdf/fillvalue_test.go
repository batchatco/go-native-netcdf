package cdf

// Unit tests for makeFillValueReader default fill values (no user-supplied _FillValue).
// Passes an empty reader so all output bytes come from the fill value path.

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

func readFillBytes(t *testing.T, vtype uint32, n int) []byte {
	t.Helper()
	v := variable{vType: vtype, attrs: emptyAttrs(t)}
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
			got := readFillBytes(t, tc.vtype, len(tc.want))
			if !bytes.Equal(got, tc.want) {
				t.Errorf("default fill for %s: got %x, want %x", tc.name, got, tc.want)
			}
		})
	}
}
