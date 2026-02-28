# go-native-netcdf

## Introduction

This is a native implementation of NetCDF in the Go language. It supports the CDF
and HDF5 (NetCDF4) file formats for both reading and writing. It's not a wrapper.
There's no C or C++ code underneath this. It is pure Go code. It benefits from the
the sandboxing and garbage collection that Go provides, so is safer to use in a production
environment.

The API supports both reading and writing files.
Please use the generic *Open*, *New()*, and *OpenWriter()* interfaces, rather than any lower
layer interfaces.

The goal of the API is to be easy to use, and some functionality may be compromised because
of that.

## Mapping of Types

Most types are what you what you would expect and map one-to-one to Go language types.
The only tricky ones are *bytes, unsigned bytes and strings*. The NetCDF *byte* type is
signed and the Go language *byte* type is unsigned, so the proper mapping is for NetCDF *bytes*
to become Go *int8s*. Conversely, the NetCDF *ubyte* becomes the Go *uint8*.

The *char* type in NetCDF is meant for strings, but *char* is a scalar in NetCDF and Go has
no scalar character type, just a *string* type to represent character strings. So, the
mapping of NetCDF char is to Go string as the closest fit. Scalar characters in NetCDF
will be returned as strings of length one.  Scalar characters cannot be written to NetCDF
with this API; they will always be written as strings of length one.

Also note, that while it is normal to use the *int* type in Go, this type cannot be written
to NetCDF. *int32* should be used instead.


| NetCDF type      | Go type |
|------------------|---------|
| byte             | int8    |
| ubyte            | uint8   |
| char             | string  |
| short            | int16   |
| ushort           | uint16  |
| int              | int32   |
| uint             | uint32  |
| int64            | int64   |
| uint64           | uint64  |
| float            | float32 |
| double           | float64 |


## Examples

### Reading a NetCDF file (CDF format)
```go
package main

import (
    "fmt"
    "github.com/batchatco/go-native-netcdf/netcdf"
)

func main() {
    // Open the file
    nc, err := netcdf.Open("data.nc")
    if err != nil {
        panic(err)
    }
    defer nc.Close()

    // Read the NetCDF variable from the file
    vr, _ := nc.GetVariable("latitude")
    if vr == nil {
        panic("latitude variable not found")
    }

    // Cast the data into a Go type we can use
    lats, has := vr.Values.([]float32)
    if !has {
        panic("latitude data not found")
    }
    for i, lat := range lats {
        fmt.Println(i, lat)
    }
}

```

### Reading a NetCDF file (HDF5 format)
It is similar, but supports subgroups.

```go
package main

import (
    "fmt"
    "github.com/batchatco/go-native-netcdf/netcdf"
)

func main() {
    // Open the file
    ncf, err := netcdf.Open("data.nc")
    if err != nil {
        panic(err)
    }
    defer ncf.Close()

    // This is the only thing different about HDF5 from CDF
    // in this implementation.
    nc, err := ncf.GetGroup("/raw")
    if err != nil {
        panic(err)
    }
    defer nc.Close()

    // Read the NetCDF variable from the file
    vr, _ := nc.GetVariable("latitude")
    if vr == nil {
        panic("latitude variable not found")
    }

    // Cast the data into a Go type we can use
    lats, has := vr.Values.([]float32)
    if !has {
        panic("latitude data not found")
    }
    for i, lat := range lats {
        fmt.Println(i, lat)
    }
}

```

### Handling Large Variables (Slicing)

For very large variables that may not fit in memory, you can use the `VarGetter` interface to read specific slices. This is highly optimized and only reads the necessary data from disk.

```go
    // Get a VarGetter for a large variable
    vg, err := nc.GetVarGetter("elevation")
    if err != nil {
        panic(err)
    }

    // Get the shape (dimensions) of the variable
    shape := vg.Shape()
    fmt.Println("Shape:", shape) // e.g., [43200 86400]

    // Read a 1D slice (e.g., first 100 rows)
    // GetSlice(begin, end)
    data, err := vg.GetSlice(0, 100)

    // Read a multi-dimensional slice (e.g., a 100x100 region at the top-left)
    // GetSliceMD(beginIndices, endIndices)
    dataMD, err := vg.GetSliceMD([]int64{0, 0}, []int64{100, 100})
```

### Writing a NetCDF file
```go

package main

import (
  "github.com/batchatco/go-native-netcdf/netcdf"
  "github.com/batchatco/go-native-netcdf/netcdf/api"
  "github.com/batchatco/go-native-netcdf/netcdf/util"
)

func main() {
    // Create a new CDF file (use netcdf.KindHDF5 for HDF5)
    cw, err := netcdf.OpenWriter("newdata.nc", netcdf.KindCDF)
    if err != nil {
        panic(err)
    }

    latitude := []float32{32.5, 64.1}
    dimensions := []string{"sounding_id"}
    attributes, err := util.NewOrderedMap(
        []string{"comment"},
        map[string]any{"comment": "Latitude indexed by sounding ID"})
    if err != nil {
        panic(err)
    }
    variable := api.Variable{
        Values:     latitude,
        Dimensions: dimensions,
        Attributes: attributes}
    err = cw.AddVar("latitude", variable)
    if err != nil {
        panic(err)
    }

    // Creating groups (HDF5 only)
    group, err := cw.CreateGroup("raw")
    if err == nil {
        group.AddVar("latitude", variable)
    } else {
        fmt.Println("CDF cannot create groups")
    }

    // Close will write out the data and close the file
    err = cw.Close()
    if err != nil {
        panic(err)
    }
}
```

## Limitations on the CDF writer
Unlimited data types are not supported. The only exception is
that a one dimensional empty slice will be written out as unlimited, but
currently zero length. For writing out variables with dimensions greater than
one to work, extra information would need to be passed in to know the sizes of
the other dimensions, because they cannot be guessed based upon the information
in the slice. This doesn't seem like all that important of a feature though,
and it would clutter the API, so it is not implemented.

## Some notes about the HDF5 code
The HDF5 code has been optimized for performance, especially when using the slicing
APIs (`GetSlice` and `GetSliceMD`). These APIs only read the required data from disk, making
it possible to work with very large datasets that can otherwise exceed available memory.

The implementation is pure Go and has good test coverage.

This implementation focuses on supporting NetCDF4 only. While it uses HDF5 as the
underlying format for NetCDF4, it does not aim to support every feature or exotic
data type available in the full HDF5 specification that is not typically used in
NetCDF4 files.

### HDF5 Reader Type Support
The HDF5 reader supports user-defined types such as enum, compound, opaque, and
vlen. However, the values returned for these types use internal (unexported) Go
types, so they cannot be inspected programmatically by callers. The `ListTypes()`
and `GetType()` methods on the `Group` interface provide CDL-format type
descriptions as strings, which is currently the only way to get metadata such as
enum member names.

### HDF5 Writer Type Support
The HDF5 writer supports all standard NetCDF4 types listed in the type mapping
table above. User-defined types such as enum, compound, opaque, and vlen are not
directly constructable through the writer API, but if read from an existing file,
they can be written back out (round-tripped) without loss.

### HDF5 Writer Storage
The native HDF5 writer currently stores all data using **contiguous** storage.
Advanced HDF5 features such as **chunking**, **deflate (compression)**, **shuffle**, and **Fletcher32** are not supported for writing.
The utility *h5repack* can be used if these features are important to you.

Benchmarks (both using this library and the official NetCDF C API) have shown that
contiguous storage performs just as well as uncompressed chunked storage for common
NetCDF access patterns. Though, your mileage may vary.

If you want to run the HDF5 unit tests, you will need the *netcdf* package (for *ncgen*
and *ncdump*) and the HDF5 tools package (for *h5dump* and *h5repack*). Both are available
as Ubuntu packages.

*ncgen* generates NetCDF4 test files from CDL templates. The writer roundtrip tests use
*ncdump* to verify that written files are valid NetCDF4, *h5dump* to check HDF5 structural
validity, and *h5repack* to roundtrip files through the HDF5 C library.

```console
$ sudo apt-get install netcdf-bin
$ sudo apt-get install hdf5-tools
```
