package hdf5

import (
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

func TestSlicer(t *testing.T) {
	genName := ncGen(t, "testslicer")
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	tidValues := [][]int8{
		{0, 1, 2, 3, 4},
		{5, 6, 6, 8, 9},
		{10, 11, 12, 13, 14},
		{15, 16, 17, 18, 19}}
	contents := keyValList{
		{"tid", api.Variable{
			Values:     tidValues,
			Attributes: nilMap,
			Dimensions: []string{"lat", "lon"}}},
	}

	// Now read and verify it
	nc, err := Open(genName)
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
	// Grab slices of various sizes (including 0 and 4) and see if they match expected.
	for sliceSize := 0; sliceSize <= 4; sliceSize++ {
		for i := 0; i < (4 - sliceSize); i++ {
			slice, err := slicer.GetSlice(int64(i), int64(i+sliceSize))
			if err != nil {
				t.Error(err)
				return
			}
			got := api.Variable{
				Values:     slice,
				Dimensions: slicer.Dimensions(),
				Attributes: slicer.Attributes()}
			tid := contents[0]
			exp := keyValList{
				{tid.name, api.Variable{
					Values:     tidValues[i : i+sliceSize],
					Dimensions: tid.val.Dimensions,
					Attributes: tid.val.Attributes}},
			}
			exp.check(t, "tid", got)
		}
	}
}
