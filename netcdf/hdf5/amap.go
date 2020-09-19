package hdf5

import (
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

// Adds callbacks to the standard OrderedMap so we can print
// out more complex types than the default (structs, enums, etc.)
type callback struct {
	h5 *HDF5
	om *util.OrderedMap
}

// CDL callback (regular)
func (c *callback) regCallback(key string) (string, bool) {
	ret := c.h5.findGlobalAttrType(key)
	return ret, ret != ""
}

func (c *callback) goCallback(key string) (string, bool) {
	ret := c.h5.findGlobalAttrGoType(key)
	return ret, ret != ""
}

func newTypedAttributeMap(h5 *HDF5, keys []string, values map[string]interface{}) (*util.OrderedMap, error) {
	om, err := util.NewOrderedMap(keys, values)
	assertError(err == nil, err, "creating ordered map")
	if h5 != nil {
		c := callback{h5, om}
		om.SetTypeCallbacks(c.regCallback, c.goCallback)
	}
	return om, nil
}
