package cdf

import (
	"os"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

func TestSlicer(t *testing.T) {
	// Create the file data
	fileName := "testdata/testslicer.nc"
	_ = os.Remove(fileName)
	cw, err := OpenWriter(fileName)
	defer os.Remove(fileName)
	defer closeCW(t, &cw, fileName) // can be called twice
	if err != nil {
		t.Error(err)
		return
	}
	tidValues := [][]uint8{
		{0, 1, 2, 3, 4},
		{5, 6, 6, 8, 9},
		{10, 11, 12, 13, 14},
		{15, 16, 17, 18, 19}}
	contents := keyValList{
		{"tid", "ubyte", "uint8", api.Variable{
			Values:     tidValues,
			Attributes: nilMap,
			Dimensions: []string{"lat", "lon"}}},
	}
	for i := range contents {
		err := cw.AddVar(contents[i].name, contents[i].val)
		if err != nil {
			t.Error(err)
			return
		}
	}
	closeCW(t, &cw, fileName) // this writes out the data

	// Now read and verify it
	nc, err := Open(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()

	slicer, err := nc.GetVarGetter("tid")
	if err != nil {
		t.Error(err)
		return
	}

	varType := slicer.Type()
	if varType != "ubyte" {
		t.Error("Type() is wrong", varType)
	}

	varType = slicer.GoType()
	if varType != "uint8" {
		t.Error("GoType() is wrong", varType)
	}

	// Grab slices of various sizes (including 0 and 4) and see if they match expected.
	for sliceSize := 0; sliceSize <= 4; sliceSize++ {
		for i := range 4 - sliceSize {
			slice, err := slicer.GetSlice(int64(i), int64(i+sliceSize))
			if err != nil {
				t.Error(err)
				return
			}
			tid := contents[0]
			exp := keyValList{
				{tid.name, "ubyte", "uint8", api.Variable{
					Values:     tidValues[i : i+sliceSize],
					Dimensions: tid.val.Dimensions,
					Attributes: tid.val.Attributes}},
			}
			if !exp.check(t, "tid", slicer, slice) {
				t.Error("value mismatch", "sliceSize=", sliceSize)
				return
			}
		}
	}
}
