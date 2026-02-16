package hdf5

import (
	"os"
	"reflect"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

func TestVLenStringWriter(t *testing.T) {
	fileName := "testvlenwriter.nc"
	defer os.Remove(fileName)

	w, err := OpenWriter(fileName)
	if err != nil {
		t.Fatal(err)
	}

	values := []string{"short", "a much longer string than the first one", "medium string"}
	err = w.AddVar("vlen_var", api.Variable{
		Values:     values,
		Dimensions: []string{"dim1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	
	attrValues := []string{"attr1", "longer attribute value"}
	am, _ := util.NewOrderedMap([]string{"vlen_attr"}, map[string]interface{}{
		"vlen_attr": attrValues,
	})
	err = w.AddAttributes(am)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read it back
	r, err := Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Check variable
	v, err := r.GetVariable("vlen_var")
	if err != nil {
		t.Fatal(err)
	}

	gotValues, ok := v.Values.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", v.Values)
	}

	if !reflect.DeepEqual(gotValues, values) {
		t.Errorf("expected %v, got %v", values, gotValues)
	}
	
	// Check attribute
	attr, has := r.Attributes().Get("vlen_attr")
	if !has {
		t.Fatal("expected attribute vlen_attr")
	}
	gotAttrValues, ok := attr.([]string)
	if !ok {
		t.Fatalf("expected []string for attribute, got %T", attr)
	}
	if !reflect.DeepEqual(gotAttrValues, attrValues) {
		t.Errorf("expected %v, got %v", attrValues, gotAttrValues)
	}
}

func TestVLenStringSingleWriter(t *testing.T) {
	// Single strings should still be fixed-length according to our heuristic
	fileName := "testvlensinglewriter.nc"
	defer os.Remove(fileName)

	w, err := OpenWriter(fileName)
	if err != nil {
		t.Fatal(err)
	}

	value := "single string"
	err = w.AddVar("single_var", api.Variable{
		Values:     value,
		Dimensions: nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read it back
	r, err := Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	v, err := r.GetVariable("single_var")
	if err != nil {
		t.Fatal(err)
	}

	gotValue, ok := v.Values.(string)
	if !ok {
		t.Fatalf("expected string, got %T", v.Values)
	}

	if gotValue != value {
		t.Errorf("expected %v, got %v", value, gotValue)
	}
}
