package hdf5

import (
	"os"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

func TestInvalidName(t *testing.T) {
	fileName := "testinvalidname.h5"
	defer os.Remove(fileName)

	w, err := OpenWriter(fileName)
	if err != nil {
		t.Fatal(err)
	}

	invalidNames := []string{
		"_invalid", // Starts with underscore (reserved)
		"invalid/name",
		"invalid\x01name",
		"trailing ",
	}

	for _, name := range invalidNames {
		err = w.AddVar(name, api.Variable{
			Values:     int32(1),
			Dimensions: nil,
			Attributes: nil,
		})
		// If it doesn't return an error, then it's currently allowing invalid names
		if err == nil {
			t.Errorf("Expected error for invalid variable name %q, but got nil", name)
		}
	}

	for _, name := range invalidNames {
		_, err = w.CreateGroup(name)
		if err == nil {
			t.Errorf("Expected error for invalid group name %q, but got nil", name)
		}
	}

	// Test invalid attribute name
	keys := []string{"invalid/name"}
	vals := map[string]any{"invalid/name": int32(1)}
	attrs, _ := util.NewOrderedMap(keys, vals)
	err = w.AddAttributes(attrs)
	if err == nil {
		t.Error("Expected error for invalid attribute name, but got nil")
	}

	// Test that underscore-prefixed names are NOT allowed for users
	keys = []string{"_FillValue"}
	vals = map[string]any{"_FillValue": int32(1)}
	attrs, _ = util.NewOrderedMap(keys, vals)
	err = w.AddAttributes(attrs)
	if err == nil {
		t.Error("Expected error for underscore-prefixed attribute name from user, but got nil")
	}
}
