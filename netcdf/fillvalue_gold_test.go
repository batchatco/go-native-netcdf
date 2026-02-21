package netcdf

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/batchatco/go-native-netcdf/internal"
)

func TestFillValueGoldStandard(t *testing.T) {
	// CDL with no explicit _FillValue attributes
	cdl := `
netcdf gold {
dimensions:
	dim = 2 ;
variables:
	byte b(dim) ;
	ubyte ub(dim) ;
	short s(dim) ;
	ushort us(dim) ;
	int i(dim) ;
	uint ui(dim) ;
	int64 i64(dim) ;
	uint64 ui64(dim) ;
	float f(dim) ;
	double d(dim) ;
data:
	b = _, _ ;
	ub = _, _ ;
	s = _, _ ;
	us = _, _ ;
	i = _, _ ;
	ui = _, _ ;
	i64 = _, _ ;
	ui64 = _, _ ;
	f = _, _ ;
	d = _, _ ;
}
`
	formats := []struct {
		name string
		flag string
	}{
		{"NetCDF4", "-4"},
		{"NetCDF5", "-5"},
	}

	for _, fmt := range formats {
		t.Run(fmt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			cdlPath := filepath.Join(tmpDir, "gold.cdl")
			ncPath := filepath.Join(tmpDir, "gold.nc")

			if err := os.WriteFile(cdlPath, []byte(cdl), 0644); err != nil {
				t.Fatal(err)
			}

			// Use ncgen to create the file.
			cmd := exec.Command("ncgen", fmt.flag, "-o", ncPath, cdlPath)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("ncgen failed for format %s: %v\n%s", fmt.name, err, out)
			}

			nc, err := Open(ncPath)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()

			// Expected values based on NetCDF standard
			expected := []struct {
				name string
				want any
			}{
				{"b", []int8{int8(internal.FillByte), int8(internal.FillByte)}},
				{"ub", []uint8{internal.FillUByte, internal.FillUByte}},
				{"s", []int16{int16(internal.FillShort), int16(internal.FillShort)}},
				{"us", []uint16{internal.FillUShort, internal.FillUShort}},
				{"i", []int32{int32(internal.FillInt), int32(internal.FillInt)}},
				{"ui", []uint32{internal.FillUInt, internal.FillUInt}},
				{"i64", []int64{internal.FillInt64, internal.FillInt64}},
				{"ui64", []uint64{internal.FillUInt64, internal.FillUInt64}},
				{"f", []float32{math.Float32frombits(internal.FillFloat), math.Float32frombits(internal.FillFloat)}},
				{"d", []float64{math.Float64frombits(internal.FillDouble), math.Float64frombits(internal.FillDouble)}},
			}

			for _, tc := range expected {
				t.Run(tc.name, func(t *testing.T) {
					if fmt.name == "NetCDF5" && tc.name == "i64" {
						t.Skip("ncgen -5 incorrectly writes int64 as int32 (vType 4)")
					}

					v, err := nc.GetVariable(tc.name)
					if err != nil {
						t.Fatal(err)
					}
					
					match := reflect.DeepEqual(v.Values, tc.want)
					if !match {
						// Special check for floats to distinguish between different NaNs
						if got, ok := v.Values.([]float32); ok {
							want := tc.want.([]float32)
							if len(got) == len(want) {
								match = true
								for i := range got {
									if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
										match = false
										break
									}
								}
							}
						} else if got, ok := v.Values.([]float64); ok {
							want := tc.want.([]float64)
							if len(got) == len(want) {
								match = true
								for i := range got {
									if math.Float64bits(got[i]) != math.Float64bits(want[i]) {
										match = false
										break
									}
								}
							}
						}
					}

					if !match {
						t.Errorf("Format %s, Variable %s: got %v, want %v", fmt.name, tc.name, v.Values, tc.want)
					}
				})
			}
		})
	}
}
