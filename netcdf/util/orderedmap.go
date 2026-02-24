// Package util is to create AttributeMaps for the NetCDF API
package util

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

type funcCallback func(name string) (string, bool)

type OrderedMap struct {
	keys            []string
	values          map[string]any
	visibleKeys     []string
	hiddenKeys      map[string]bool
	regTypeCallback funcCallback
	goTypeCallback  funcCallback
}

var (
	ErrorKeysDontMatchValues = errors.New("keys don't match values")
)

// NewOrderedMap takes an unordered map (values) and an order (keys) and
// returns an OrderedMap.
func NewOrderedMap(keys []string, values map[string]any) (*OrderedMap, error) {
	if len(keys) != len(values) {
		return nil, ErrorKeysDontMatchValues
	}
	mapKeys := []string{}
	for k := range values {
		mapKeys = append(mapKeys, k)
	}
	slices.Sort(mapKeys)

	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	slices.Sort(sortedKeys)

	for i := range sortedKeys {
		if mapKeys[i] != sortedKeys[i] {
			return nil, ErrorKeysDontMatchValues
		}
	}
	if values == nil {
		values = map[string]any{}
	}

	visibleKeys := []string{}
	visibleKeys = append(visibleKeys, keys...)

	return &OrderedMap{
		keys:        keys,
		values:      values,
		visibleKeys: visibleKeys,
		hiddenKeys:  map[string]bool{}}, nil
}

// Add adds a key/value pair to an ordered map at the end.
func (om *OrderedMap) Add(name string, val any) {
	if !om.hiddenKeys[name] {
		om.keys = append(om.keys, name)
		om.visibleKeys = append(om.visibleKeys, name)
	}
	om.values[name] = val
}

// Get returns the value associated with key and sets has to true if found.
func (om *OrderedMap) Get(key string) (val any, has bool) {
	val, has = om.values[key]
	return
}

// SetTypeCallbacks allows one to override the default type behavior.
// The default type calculation doesn't work with user-defined types.
// It's okay for CDF though, but not HDF5.
func (om *OrderedMap) SetTypeCallbacks(regCb funcCallback, goCb funcCallback) {
	om.regTypeCallback = regCb
	om.goTypeCallback = goCb
}

// Hide hides the given key from the Keys() method, but
// it is still in the map.
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

// Keys returns the keys in order for the map.
// It does not return the hidden keys.
func (om *OrderedMap) Keys() []string {
	return om.visibleKeys
}

func (om *OrderedMap) pGoType(val any) (string, bool) {
	rVal := reflect.ValueOf(val)
	var prelim string
	switch rVal.Kind() {
	case reflect.Array:
		var i any
		if rVal.Len() == 0 {
			i = reflect.Zero(rVal.Type().Elem()).Interface()
		} else {
			i = rVal.Index(0).Interface()
		}
		inner, has := om.pGoType(i)
		if !has {
			return "", false
		}
		prelim = fmt.Sprintf("[%d]%s", rVal.Len(), inner)
	case reflect.Slice:
		var i any
		if rVal.Len() == 0 {
			i = reflect.Zero(rVal.Type().Elem()).Interface()
		} else {
			i = rVal.Index(0).Interface()
		}
		inner, has := om.pGoType(i)
		if !has {
			return "", false
		}
		prelim = fmt.Sprintf("[]%s", inner)
	case reflect.Struct:
		inner := ""
		for i := range rVal.NumField() {
			field := rVal.Type().Field(i)
			fName := field.Name
			var fType string
			var has bool
			var fi any
			if field.PkgPath != "" {
				ft := rVal.Field(i).Type()
				fi = reflect.Zero(ft).Interface()
			} else {
				fi = rVal.Field(i).Interface()
			}
			fType, has = om.pGoType(fi)
			if !has {
				return "", false
			}
			if inner != "" {
				inner = inner + ";"
			}
			inner = inner + fmt.Sprintf(" %s %s", fName, fType)
		}
		prelim = fmt.Sprintf("struct {%s }", inner)
	default:
		return rVal.Kind().String(), true
	}
	return prelim, true
}

// GetGoType returns the Go description of the given variable and sets the
// bool to true if found.
func (om *OrderedMap) GetGoType(key string) (ty string, has bool) {
	val, has := om.Get(key)
	if !has {
		return "", false
	}
	if om.goTypeCallback != nil {
		return om.goTypeCallback(key)
	}
	return om.pGoType(val)
}

// We end up with dimensions not being in the right CDL format
// from pType.
// It can look like this: int(5)(1,2,3)
func (om *OrderedMap) putDim(inner string, dim int, kind reflect.Kind) string {
	var sDim string
	if dim == 0 && kind == reflect.Slice {
		sDim = "*"
	} else {
		sDim = strconv.Itoa(dim)
	}
	parenLoc := strings.LastIndex(inner, "(")
	if parenLoc == -1 || parenLoc == len(inner)-1 {
		return fmt.Sprintf("%s(%s)", inner, sDim)
	}
	fixed := fmt.Sprintf("%s%s,%s", inner[:parenLoc+1], sDim, inner[parenLoc+1:])
	return fixed
}

func (om *OrderedMap) pType(val any) (string, bool) {
	rVal := reflect.ValueOf(val)
	rTyp := reflect.TypeOf(val)
	prelim := ""
	switch rVal.Kind() {
	case reflect.Array:
		var i any
		if rVal.Len() == 0 {
			i = reflect.Zero(rTyp.Elem()).Interface()
		} else {
			i = rVal.Index(0).Interface()
		}
		inner, has := om.pType(i)
		if !has || inner == "" {
			return "", false
		}
		prelim = om.putDim(inner, rVal.Len(), rVal.Kind())
	case reflect.Slice:
		var i any
		if rVal.Len() == 0 {
			i = reflect.Zero(rTyp.Elem()).Interface()
		} else {
			i = rVal.Index(0).Interface()
		}
		inner, has := om.pType(i)
		if !has {
			return "", false
		}
		prelim = om.putDim(inner, rVal.Len(), rVal.Kind())

	case reflect.Struct:
		inner := ""
		for i := range rVal.NumField() {
			field := rTyp.Field(i)
			fName := field.Name
			var fType string
			var has bool
			var fi any
			if field.PkgPath != "" {
				ft := rVal.Field(i).Type()
				fi = reflect.Zero(ft).Interface()
			} else {
				fi = rVal.Field(i).Interface()
			}
			fType, has = om.pType(fi)
			if !has {
				return "", false
			}
			inner = inner + fmt.Sprintf(" %s %s;", fType, fName)
		}
		prelim = fmt.Sprintf("compound {%s }", inner)
	default:
		kind := rVal.Kind().String()
		replacements := map[string]string{
			"float32": "float",
			"float64": "double",
			"int8":    "byte",
			"int16":   "short",
			"int32":   "int",
			"uint8":   "ubyte",
			"uint16":  "ushort",
			"uint32":  "uint",
			"string":  "string",
		}
		rep, has := replacements[kind]
		if !has {
			return kind, true // named or complex type: return as-is
		}
		return rep, true
	}
	return prelim, true
}

// GetType returns the CDL description of the given variable and sets the
// bool to true if found.
func (om *OrderedMap) GetType(key string) (ty string, has bool) {
	val, has := om.Get(key)
	if !has {
		return "", false
	}
	if om.regTypeCallback != nil {
		return om.regTypeCallback(key)
	}

	prelim, has := om.pType(val)
	if !has {
		return "", false
	}
	// Put in the commas.
	return strings.ReplaceAll(prelim, ")(", ","), true
}
