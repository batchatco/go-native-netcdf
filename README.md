# go-native-netcdf

## Introduction

This is a native implementation of NetCDF in the Go language.  It only supports the CDF
file format at the current time. HDF5 support is forthcoming.

The API is mainly intended for reading files, though there is support for writing CDF files.
To read files, please use the generic NewNetCDF4() interface, rather than any lower layer interfaces.

The goal of the API is to be easy to use, and some functionality may be compromised.

## Mapping of Types

Most types are what you what you would expect and map one-to-one to Go language types.  The only tricky ones are bytes, unsigned bytes and strings.  Integers are slightly tricky as well.


| CDF type      | Go type |
|---------------|---------|
| byte          | int8    |
| unsigned byte | uint8   |
| char          | string  |
| int           | int32   |




