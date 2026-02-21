package hdf5

// Tests for the defaultFillValue method on each type manager.
// These are pass-through for most types (return the provided fill value
// unchanged) and always-zero for string/vlen types.

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDefaultFillValuePassthrough(t *testing.T) {
	obj := &object{}
	in := []byte{0xAA, 0xBB, 0xCC}

	cases := []struct {
		name string
		mgr  typeManager
	}{
		{"array", arrayManager},
		{"compound", compoundManager},
		{"enum", enumManager},
		{"opaque", opaqueManager},
		{"reference", referenceManager},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.mgr.defaultFillValue(obj, in, false)
			if !bytes.Equal(got, in) {
				t.Errorf("defaultFillValue(%s): got %v, want %v (passthrough)", tc.name, got, in)
			}
			got2 := tc.mgr.defaultFillValue(obj, in, true)
			if !bytes.Equal(got2, in) {
				t.Errorf("defaultFillValue(%s, undefined=true): got %v, want %v", tc.name, got2, in)
			}
		})
	}
}

func TestDefaultFillValueString(t *testing.T) {
	obj := &object{}
	want := []byte{0}

	// String always returns {0} regardless of inputs.
	for _, undefined := range []bool{false, true} {
		for _, in := range [][]byte{nil, {}, {1, 2, 3}} {
			got := stringManager.defaultFillValue(obj, in, undefined)
			if !bytes.Equal(got, want) {
				t.Errorf("stringManager.defaultFillValue(in=%v, undefined=%v): got %v, want %v",
					in, undefined, got, want)
			}
		}
	}
}

func TestDefaultFillValueNilInput(t *testing.T) {
	obj := &object{}

	// Passthrough types return nil when given nil.
	cases := []struct {
		name string
		mgr  typeManager
	}{
		{"array", arrayManager},
		{"compound", compoundManager},
		{"enum", enumManager},
		{"opaque", opaqueManager},
		{"reference", referenceManager},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.mgr.defaultFillValue(obj, nil, false)
			if got != nil {
				t.Errorf("defaultFillValue(%s, nil): got %v, want nil", tc.name, got)
			}
		})
	}
}

func TestDefaultFillValueFixedPoint(t *testing.T) {
	cases := []struct {
		length uint32
		want   []byte
		signed bool
	}{
		{1, []byte{0x81}, true},                                         // -127
		{1, []byte{0xff}, false},                                        // 255
		{2, []byte{0x01, 0x80}, true},                                   // -32767
		{2, []byte{0xff, 0xff}, false},                                  // 65535
		{4, []byte{0x01, 0x00, 0x00, 0x80}, true},                       // -2147483647
		{4, []byte{0xff, 0xff, 0xff, 0xff}, false},                      // 4294967295
		{8, []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}, true}, // -9223372036854775806
		{8, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, false}, // 18446744073709551614
	}
	for _, tc := range cases {
		obj := &object{objAttr: &attribute{length: tc.length, endian: binary.LittleEndian, signed: tc.signed}}
		got := fixedPointManager.defaultFillValue(obj, nil, true)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("fixedPointManager.defaultFillValue(length=%d, signed=%v, undefined=true): got %x, want %x",
				tc.length, tc.signed, got, tc.want)
		}
	}
}

func TestDefaultFillValueFloatingPoint(t *testing.T) {
	cases := []struct {
		length uint32
		want   []byte
	}{
		{4, []byte{0x00, 0x00, 0xf0, 0x7c}},                         // 0x7cf00000 in LittleEndian
		{8, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x9e, 0x47}}, // 0x479e000000000000 in LittleEndian
	}
	for _, tc := range cases {
		obj := &object{objAttr: &attribute{length: tc.length, endian: binary.LittleEndian}}
		got := floatingPointManager.defaultFillValue(obj, nil, true)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("floatingPointManager.defaultFillValue(length=%d, undefined=true): got %x, want %x",
				tc.length, got, tc.want)
		}
	}
}

func TestDefaultFillValueVlen(t *testing.T) {
	obj := &object{}
	want := []byte{0}
	got := vlenManager.defaultFillValue(obj, nil, true)
	if !bytes.Equal(got, want) {
		t.Errorf("vlenManager.defaultFillValue: got %x, want %x", got, want)
	}
}
