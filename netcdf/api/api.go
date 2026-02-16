// Package api is common to different implementations of NetCDF4 (CDF or HDF5)
package api

import (
	"io"
)

type ReadSeekerCloser interface {
	io.ReadSeeker
	io.Closer
}

type AttributeMap interface {
	// Ordered list of keys
	Keys() []string
	// Indexed lookup
	Get(key string) (val any, has bool)

	GetType(key string) (string, bool)
	GetGoType(key string) (string, bool)
}

type Variable struct {
	Values     any
	Dimensions []string
	Attributes AttributeMap
}

type VarGetter interface {
	// Len() is the total length of the variable's slice.
	// Or returns 1 if it is a scalar.
	Len() int64

	// Values returns all the values of the variable.  For very large variables,
	// it may be more appropriate to call GetSlice instead.
	Values() (any, error)

	// GetSlice gets a (smaller) slice of the variable's slice
	// It's useful for variables which are very large and may not fit in memory.
	GetSlice(begin, end int64) (any, error)

	// GetSliceMD gets a multi-dimensional slice of the variable.
	// begin and end are slices of indices for each dimension.
	GetSliceMD(begin, end []int64) (any, error)

	// Shape returns the lengths of all dimensions of the variable.
	Shape() []int64

	Dimensions() []string

	Attributes() AttributeMap

	// Type returns the base type in CDL format, not including dimensions.
	Type() string
	// GoType returns the base type in Go format, not including dimensions.
	GoType() string
}

type Group interface {
	// Close closes this group and closes any underlying files if they are no
	// longer being used by any other groups.
	Close()

	// Attributes returns the global attributes for this group.
	Attributes() AttributeMap

	// ListVariables lists the variables in this group.
	ListVariables() []string

	// GetVariable returns the named variable or sets the error if not found.
	GetVariable(name string) (*Variable, error)

	// GetVarGetter is an function that returns an interface that allows you to get
	// smaller slices of a variable, in case the variable is very large and you want to
	// reduce memory usage.
	GetVarGetter(name string) (VarGetter, error)

	// ListSubgroups returns the names of the subgroups of this group
	ListSubgroups() []string

	// GetGroup gets the given group or returns an error if not found.
	// The group can start with "/" for absolute names, or relative.
	GetGroup(group string) (g Group, err error)

	// Experimental API to get user-defined type information

	// ListTypes returns the user-defined type names.
	ListTypes() []string

	// GetType gets the CDL description of the type and sets the bool to true if found.
	GetType(string) (string, bool)

	// GettGoType gets the Go description of the type and sets the bool to true if found.
	GetGoType(string) (string, bool)

	// ListDimensions lists the names of the dimensions in this group.
	ListDimensions() []string

	// GetDimension returns the size of the given dimension and sets
	// the bool to true if found.
	GetDimension(string) (uint64, bool)
}
