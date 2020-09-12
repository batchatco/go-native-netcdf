package util

import (
	"testing"
)

func TestNil(t *testing.T) {
	_, err := NewOrderedMap(nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = NewOrderedMap(nil, map[string]interface{}{})
	if err != nil {
		t.Error(err)
		return
	}
	_, err = NewOrderedMap([]string{}, nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMismatchedLength(t *testing.T) {
	_, err := NewOrderedMap([]string{"a", "b"},
		map[string]interface{}{"a": nil})
	if err != ErrorKeysDontMatchValues {
		t.Error("Should have returned an error")
		return
	}
}

func TestHidden(t *testing.T) {
	om, err := NewOrderedMap([]string{"a", "b"},
		map[string]interface{}{"a": nil, "b": nil})
	if err != nil {
		t.Error(err)
		return
	}
	om.Hide("a")
	keys := om.Keys()
	if len(keys) != 1 || keys[0] != "b" {
		t.Error("Hide() failed")
		return
	}
	om.Add("a", 1)
	keys = om.Keys()
	if len(keys) != 1 || keys[0] != "b" {
		t.Error("Hide() failed")
		return
	}
	om.Hide("c")
}

func TestMismatchedKeys(t *testing.T) {
	_, err := NewOrderedMap([]string{"a", "b"},
		map[string]interface{}{"a": nil, "c": nil})
	if err != ErrorKeysDontMatchValues {
		t.Error("Should have returned an error")
		return
	}
}

func TestAdd(t *testing.T) {
	om, err := NewOrderedMap(nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	om.Add("a", 1)
	val, has := om.Get("a")
	if !has {
		t.Error("Did not find expected key")
		return
	}
	if val.(int) != 1 {
		t.Error("Did not get expected value back")
		return
	}
}

var i int32
var f float32
var d float64
var s string

type sI struct {
	s string
	I int32
}

var n sI

var i1 []int32
var i2 = []int32{0}
var f1 []float32
var f2 = []float32{1, 2}
var d1 []float64
var d2 = []float64{1, 2, 3}
var s1 []string
var s2 = []string{"1", "2", "3", "4"}

type fI struct {
	f float32
	I int32
}

var n1 []fI

var n2 = []fI{
	{0, 1},
	{2, 3},
	{4, 5},
	{6, 7},
}

var i12 [2]int32
var f12 [2]float32
var d12 [2]float64
var s12 [2]string

type If struct {
	I int32
	f float32
}

var n12 [2]If

type c25i struct{ member [2][5]int32 }

var i22 c25i

type c257i struct{ member [2][5][7]int32 }

var i23 c257i

var myMap = map[string]interface{}{
	// scalars
	"i": i, "f": f, "d": d, "s": s,
	"n": n,
	// empty slices
	"i1": i1, "f1": f1, "d1": d1, "s1": s1,
	"n1": n1,
	// filled slices
	"i2": i2, "f2": f2, "d2": d2, "s2": s2,
	"n2": n2,
	// arrays can only be defined as compound fields
	"i12": i12, "f12": f12, "d12": d12, "s12": s12,
	"n12": n12,
	// 2-dimensional
	"i22": i22,
	// 3-dimensional
	"i23": i23,
}
var om *OrderedMap

func initMaps(t *testing.T) {
	var err error
	om, err = NewOrderedMap([]string{
		"i", "f", "d", "s",
		"n",
		"i1", "f1", "d1", "s1",
		"n1",
		"i2", "f2", "d2", "s2",
		"n2",
		"i12", "f12", "d12", "s12",
		"n12", "i22", "i23",
	}, myMap)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestOrder(t *testing.T) {
	myMap := map[string]interface{}{"a": nil, "b": nil, "c": nil}
	om, err := NewOrderedMap([]string{"c", "b", "a"}, myMap)
	if err != nil {
		t.Error(err)
		return
	}
	keys := om.Keys()
	if keys[0] != "c" || keys[1] != "b" || keys[2] != "a" {
		t.Error("Incorrect key order:", keys)
	}
}

func TestGoType(t *testing.T) {
	initMaps(t)
	rightTypes := map[string]string{
		"i":   "int32",
		"f":   "float32",
		"d":   "float64",
		"s":   "string",
		"n":   "struct { s string; I int32 }",
		"i1":  "[]int32",
		"f1":  "[]float32",
		"d1":  "[]float64",
		"s1":  "[]string",
		"n1":  "[]struct { f float32; I int32 }",
		"i2":  "[]int32",
		"f2":  "[]float32",
		"d2":  "[]float64",
		"s2":  "[]string",
		"n2":  "[]struct { f float32; I int32 }",
		"i12": "[2]int32",
		"f12": "[2]float32",
		"d12": "[2]float64",
		"s12": "[2]string",
		"n12": "[2]struct { I int32; f float32 }",
		"i22": "struct { member [2][5]int32 }",
		"i23": "struct { member [2][5][7]int32 }",
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
	initMaps(t)
	rightTypes := map[string]string{
		"i":   "int",
		"f":   "float",
		"d":   "double",
		"s":   "string",
		"n":   "compound { string s; int I; }",
		"i1":  "int(*)",
		"f1":  "float(*)",
		"d1":  "double(*)",
		"s1":  "string(*)",
		"n1":  "compound { float f; int I; }(*)",
		"i2":  "int(1)",
		"f2":  "float(2)",
		"d2":  "double(3)",
		"s2":  "string(4)",
		"n2":  "compound { float f; int I; }(4)",
		"i12": "int(2)",
		"f12": "float(2)",
		"d12": "double(2)",
		"s12": "string(2)",
		"n12": "compound { int I; float f; }(2)",
		"i22": "compound { int(2,5) member; }",
		"i23": "compound { int(2,5,7) member; }",
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
