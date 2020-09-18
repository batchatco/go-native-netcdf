package hdf5

import (
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

type callback struct {
	h5 *HDF5
	om *util.OrderedMap
}

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
	if err != nil {
		return nil, err
	}
	if h5 != nil {
		c := callback{h5, om}
		om.SetTypeCallbacks(c.regCallback, c.goCallback)
	}
	return om, nil
}
