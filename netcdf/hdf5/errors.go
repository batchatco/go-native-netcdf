package hdf5

import "errors"

var (
	// ErrBadMagic is returned when the file is not an HDF5 file
	ErrBadMagic = errors.New("bad magic number")

	// ErrUnsupportedFilter is returned when an unrecognized filter is encountered
	ErrUnsupportedFilter = errors.New("unsupported filter found")

	// ErrUnsupportedCompression is returned for unsupported copmression schemes
	ErrUnknownCompression = errors.New("unknown compression")

	// ErrInternal is an internal error not otherwise specified here
	ErrInternal = errors.New("internal error")

	// ErrNotFound is returned for items requested that don't exist
	ErrNotFound = errors.New("not found")

	// ErrFletcherChecksum is returned for corrupted data failing the checksum
	ErrFletcherChecksum = errors.New("fletcher checksum failure")

	// ErrVersion is returned when the particular HDF5 version is not supported
	ErrVersion = errors.New("hdf5 version not supported")

	// ErrLinkType is returned for an unrecognized or unsupported link type
	ErrLinkType = errors.New("link type not supported")

	// ErrVirtualStorage is returned when the unsupported virtual storage feature is encountered
	ErrVirtualStorage = errors.New("virtual storage not supported")

	// ErrTruncated is returned when the file has fewer bytes than the superblock says
	ErrTruncated = errors.New("file is too small, may be truncated")

	// ErrOffsetSize is returned when offsets other than 64-bit are indicated.
	// Only 64-bit is supported in this implementation.
	ErrOffsetSize = errors.New("only 64-bit offsets are supported")

	// ErrDimensionality is returned when invalid dimensions are specified
	ErrDimensionality = errors.New("invalid dimensionality")

	// ErrDataspaceVersion is returned for unsupported dataspace versions
	ErrDataspaceVersion = errors.New("dataspace version not supported")

	// ErrCorrupted is returned when file inconsistencies are found
	ErrCorrupted = errors.New("corrupted file")

	// ErrLayout is returned for unsupported data layouts
	ErrLayout = errors.New("data layout version not supported")

	// ErrSuperblock is returned for unsupported superblock versions
	ErrSuperblock = errors.New("superblock extension not supported")

	// ErrBitfield is returned when bitfields are encountered.
	// Bitfields are valid HDF5, but not valid NetCDF4.
	ErrBitfield = errors.New("bitfields not supported")

	// ErrExternal is returned when requests for external files are encountered, which is
	// not supported.
	ErrExternal = errors.New("external data files not supported")

	// ErrFloatingPoint is returned when non-standard floating point is encountered
	ErrFloatingPoint = errors.New("non-standard floating point not handled")

	// ErrFloatingPointis returned when non-standard integers are encountered
	ErrFixedPoint = errors.New("non-standard fixed-point not handled")

	// ErrReference is returned when references are encountered.
	// References are valid HDF, but not valid NetCDF4.
	ErrReference = errors.New("unsupported reference type")

	// ErrNonExportedField is returned when a value cannot be assigned to user-supplied
	// struct because it has non-exported fields.
	ErrNonExportedField = errors.New("can't assign to non-exported field")
)
