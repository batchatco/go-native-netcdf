package util

import (
	"errors"
	"sort"
)

type OrderedMap struct {
	keys        []string
	values      map[string]interface{}
	visibleKeys []string
	hiddenKeys  map[string]bool
}

var (
	ErrorKeysDontMatchValues = errors.New("keys don't match values")
)

func NewOrderedMap(keys []string, values map[string]interface{}) (*OrderedMap, error) {
	if len(keys) != len(values) {
		return nil, ErrorKeysDontMatchValues
	}
	mapKeys := []string{}
	for k := range values {
		mapKeys = append(mapKeys, k)
	}
	sort.Strings(mapKeys)

	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	for i := range sortedKeys {
		if mapKeys[i] != sortedKeys[i] {
			return nil, ErrorKeysDontMatchValues
		}
	}
	if values == nil {
		values = map[string]interface{}{}
	}

	visibleKeys := []string{}
	visibleKeys = append(visibleKeys, keys...)

	return &OrderedMap{
		keys:        keys,
		values:      values,
		visibleKeys: visibleKeys,
		hiddenKeys:  map[string]bool{}}, nil
}

func (om *OrderedMap) Add(name string, val interface{}) {
	if !om.hiddenKeys[name] {
		om.keys = append(om.keys, name)
		om.visibleKeys = append(om.visibleKeys, name)
	}
	om.values[name] = val
}

func (om *OrderedMap) Get(key string) (val interface{}, has bool) {
	val, has = om.values[key]
	return
}

func (om *OrderedMap) Hide(hiddenKey string) {
	om.hiddenKeys[hiddenKey] = true
	// recompute visible keys
	visibleKeys := []string{}
	for _, key := range om.keys {
		if om.hiddenKeys[key] {
			continue
		}
		visibleKeys = append(visibleKeys, key)
	}
	om.visibleKeys = visibleKeys
}

func (om *OrderedMap) Keys() []string {
	return om.visibleKeys
}
