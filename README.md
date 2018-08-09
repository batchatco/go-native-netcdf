# go-native-netcdf

## Introduction

This is a native implementation of NetCDF in the Go language.  It only supports the CDF
file format at the current time. HDF5 support may be available sometime in the future.

The API is mainly intended for reading files, though there is support for writing CDF files.
To read files, please use the generic *NewNetCDF4()* interface, rather than any lower layer interfaces.

The goal of the API is to be easy to use, and some functionality may be compromised.

## Mapping of Types

Most types are what you what you would expect and map one-to-one to Go language types.
The only tricky ones are *bytes, unsigned bytes and strings*.  The NetCDF *byte* type is
signed and the Go language *byte* type is unsigned, so the proper mapping is for NetCDF *bytes* to become Go *int8s*.  Conversely, the NetCDF *ubyte* becomes the Go *uint8*.

The *char* type in NetCDF is meant for strings, but *char* is a scalar in NetCDF and Go has
no scalar character type, just a *string* type to represent character strings.  So, the
mapping of NetCDF char is to Go string as the closest fit.  Scalar characters in NetCDF
will be returned as strings of length one.   Scalar characters cannot be written to NetCDF
with this API; they will always be written as strings of length one.

Also note, that while it is normal to use the *int* type in Go, this type cannot be written
to NetCDF.  *int32* should be used instead.


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

### Reading a NetCDF file
```go
package main

import (
	"fmt"

	"github.com/batchatco/go-native-netcdf/netcdf"
)
func main() {
	// Open the file
	nc, err := netcdf.NewNetCDF4("data.nc")
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

### Writing a CDF file
```go

package main

import (
  "github.com/batchatco/go-native-netcdf/netcdf/api"
  "github.com/batchatco/go-native-netcdf/netcdf/cdf"
  "github.com/batchatco/go-native-netcdf/netcdf/util"
)

func main() {
	cw, err := cdf.NewCDFWriter("newdata.nc")
	if err != nil {
		panic(err)
	}

	latitude := []float32{32.5, 64.1}
	dimensions := []string{"sounding_id"}
	attributes, err := util.NewOrderedMap(
		[]string{"comment"},
		map[string]interface{}{"comment": "Latitude indexed by sounding ID"})
	if err != nil {
		panic(err)
	}
	variable := api.Variable{
		latitude,
		dimensions,
		attributes}
	err = cw.AddVar("latitude", variable)
	if err != nil {
		panic(err)
	}
	// Close will write out the data and close the file
	err = cw.Close()
	if err != nil {
		panic(err)
	}
}
```






