// API common to different implementations of NetCDF4 (CDF or HDF5)
package api

import "io"

type ReadSeekerCloser interface {
	io.ReadSeeker
	io.Closer
}

type AttributeMap interface {
	// Ordered list of keys
	Keys() []string
	// Indexed lookup
	Get(key string) (val interface{}, has bool)
}

type Variable struct {
	Values     interface{}
	Dimensions []string
	Attributes AttributeMap
}

type Group interface {
	Close()

	Attributes() AttributeMap

	ListVariables() []string

	GetVariable(name string) (*Variable, error)

	ListSubgroups() []string
	// group can "/" for top-level
	GetGroup(group string) (g Group, err error)

	// TODO: there should be a Dimensions() call also because some dimensions are
	// the the file, but don't get used by any variables, so can't be returned in any way.
}
