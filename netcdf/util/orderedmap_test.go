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
