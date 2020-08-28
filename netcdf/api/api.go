// Package api is common to different implementations of NetCDF4 (CDF or HDF5)
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

type VarGetter interface {
	// Len() is the total length of the variable's slice.
	// Or returns 1 if it is a scalar.
	Len() int64

	// Values returns all the values of the variable.  For very large variables,
	// it may be more appropriate to call GetSlice instead.
	Values() (interface{}, error)

	// GetSlice gets a (smaller) slice of the variable's slice
	// It's useful for variables which are very large and may not fit in memory.
	GetSlice(begin, end int64) (interface{}, error)

	Dimensions() []string

	Attributes() AttributeMap
}

type Group interface {
	Close()

	Attributes() AttributeMap

	ListVariables() []string

	GetVariable(name string) (*Variable, error)

	// GetVarGetter is an EXPERIMENTAL function that returns an interface that allows you to get
	// smaller slices of a variable, in case the variable is very large and you want to
	// reduce memory usage. This API is experimental and could change or disappear.
	GetVarGetter(name string) (VarGetter, error)

	ListSubgroups() []string

	// group can "/" for top-level
	GetGroup(group string) (g Group, err error)

	// TODO: there should be a Dimensions() call also because some dimensions are
	// in the file, but don't get used by any variables. So they can't be returned in any way.
	//
	// This is also true of user-defined types.  The names cannot be returned currently.
}
