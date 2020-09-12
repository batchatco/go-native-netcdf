package hdf5

import "testing"

func TestGoType(t *testing.T) {
	fileName := "testattrtypes"
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error("Error opening", fileName, ":", errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	om := nc.Attributes()
	rightTypes := map[string]string{
		"i":  "int32",
		"f":  "float32",
		"d":  "float64",
		"s":  "string",
		"i1": "[]int32",
		"f1": "[]float32",
		"d1": "[]float64",
		"s1": "[]string",
	}
	for v, exp := range rightTypes {
		got, has := om.GetGoType(v)
		if !has {
			t.Errorf("Var %s is missing", v)
			continue
		}
		if got != exp {
			t.Errorf("wrong type for %s: got=%s exp=%s", v, got, exp)
			continue
		}
	}
}

func TestType(t *testing.T) {
	fileName := "testattrtypes"
	genName := ncGen(t, fileName)
	if genName == "" {
		t.Error("Error opening", fileName, ":", errorNcGen)
		return
	}
	nc, err := Open(genName)
	if err != nil {
		t.Error(err)
		return
	}
	defer nc.Close()
	om := nc.Attributes()
	rightTypes := map[string]string{
		"i":  "int",
		"f":  "float",
		"d":  "double",
		"s":  "string",
		"i1": "int(*)",
		"f1": "float(*)",
		"d1": "double(*)",
		"s1": "string(*)",
	}
	for v, exp := range rightTypes {
		got, has := om.GetType(v)
		if !has {
			t.Errorf("Var %s is missing", v)
			continue
		}
		if got != exp {
			t.Errorf("wrong type for %s: got=%s exp=%s", v, got, exp)
			continue
		}
	}
}
