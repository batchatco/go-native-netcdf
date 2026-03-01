// Command mksuperblock creates an HDF5 file with a specific superblock version.
//
// V0 is the default when libver bounds are set to EARLIEST. V1 is triggered
// by additionally setting a non-default indexed storage B-tree K value via
// H5Pset_istore_k. Both use libver bounds pinned to EARLIEST/V18 to prevent
// the library from upgrading to V2. This approach is necessary because
// h5repack and other HDF5 command-line tools cannot produce V0 or V1
// superblock files.
//
// Usage: mksuperblock <0|1> <output.h5>
package main

// #cgo LDFLAGS: -lhdf5
// #include <stdlib.h>
// #include <hdf5.h>
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: mksuperblock <0|1> <output.h5>\n")
		os.Exit(1)
	}
	version := os.Args[1]
	if version != "0" && version != "1" {
		fmt.Fprintf(os.Stderr, "mksuperblock: version must be 0 or 1\n")
		os.Exit(1)
	}
	filename := C.CString(os.Args[2])
	defer C.free(unsafe.Pointer(filename))

	// File creation properties: non-default istore_k forces V1
	fcpl := C.hid_t(C.H5P_DEFAULT)
	if version == "1" {
		fcpl = C.H5Pcreate(C.H5P_FILE_CREATE)
		if fcpl < 0 {
			fatal("H5Pcreate(FILE_CREATE)")
		}
		defer C.H5Pclose(fcpl)
		if C.H5Pset_istore_k(fcpl, 64) < 0 {
			fatal("H5Pset_istore_k")
		}
	}

	// File access properties: pin to earliest to prevent V2
	fapl := C.H5Pcreate(C.H5P_FILE_ACCESS)
	if fapl < 0 {
		fatal("H5Pcreate(FILE_ACCESS)")
	}
	defer C.H5Pclose(fapl)
	if C.H5Pset_libver_bounds(fapl, C.H5F_LIBVER_EARLIEST, C.H5F_LIBVER_V18) < 0 {
		fatal("H5Pset_libver_bounds")
	}

	fid := C.H5Fcreate(filename, C.H5F_ACC_TRUNC, fcpl, fapl)
	if fid < 0 {
		fatal("H5Fcreate")
	}
	defer C.H5Fclose(fid)

	// ints dataset
	writeInt32s(fid, "ints", []C.int32_t{1, 2, 3, 4, 5})

	// Add "units" attribute to ints
	did := C.H5Dopen2(fid, C.CString("ints"), C.H5P_DEFAULT)
	if did < 0 {
		fatal("H5Dopen2")
	}
	writeStringAttr(did, "units", "meters")
	C.H5Dclose(did)

	// floats dataset
	writeFloat64s(fid, "floats", []C.double{1.5, 2.5, 3.5})

	// shorts dataset
	writeInt16s(fid, "shorts", []C.int16_t{10, 20, 30})

	// subgroup with bytes dataset
	gid := C.H5Gcreate2(fid, C.CString("grp"), C.H5P_DEFAULT, C.H5P_DEFAULT, C.H5P_DEFAULT)
	if gid < 0 {
		fatal("H5Gcreate2")
	}
	writeInt8s(gid, "bytes", []C.int8_t{7, 8, 9})
	C.H5Gclose(gid)
}

func writeInt32s(loc C.hid_t, name string, data []C.int32_t) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	dims := []C.hsize_t{C.hsize_t(len(data))}
	sid := C.H5Screate_simple(1, &dims[0], nil)
	did := C.H5Dcreate2(loc, cname, C.H5T_NATIVE_INT32, sid, C.H5P_DEFAULT, C.H5P_DEFAULT, C.H5P_DEFAULT)
	C.H5Dwrite(did, C.H5T_NATIVE_INT32, C.H5S_ALL, C.H5S_ALL, C.H5P_DEFAULT, unsafe.Pointer(&data[0]))
	C.H5Dclose(did)
	C.H5Sclose(sid)
}

func writeFloat64s(loc C.hid_t, name string, data []C.double) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	dims := []C.hsize_t{C.hsize_t(len(data))}
	sid := C.H5Screate_simple(1, &dims[0], nil)
	did := C.H5Dcreate2(loc, cname, C.H5T_NATIVE_DOUBLE, sid, C.H5P_DEFAULT, C.H5P_DEFAULT, C.H5P_DEFAULT)
	C.H5Dwrite(did, C.H5T_NATIVE_DOUBLE, C.H5S_ALL, C.H5S_ALL, C.H5P_DEFAULT, unsafe.Pointer(&data[0]))
	C.H5Dclose(did)
	C.H5Sclose(sid)
}

func writeInt16s(loc C.hid_t, name string, data []C.int16_t) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	dims := []C.hsize_t{C.hsize_t(len(data))}
	sid := C.H5Screate_simple(1, &dims[0], nil)
	did := C.H5Dcreate2(loc, cname, C.H5T_NATIVE_INT16, sid, C.H5P_DEFAULT, C.H5P_DEFAULT, C.H5P_DEFAULT)
	C.H5Dwrite(did, C.H5T_NATIVE_INT16, C.H5S_ALL, C.H5S_ALL, C.H5P_DEFAULT, unsafe.Pointer(&data[0]))
	C.H5Dclose(did)
	C.H5Sclose(sid)
}

func writeInt8s(loc C.hid_t, name string, data []C.int8_t) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	dims := []C.hsize_t{C.hsize_t(len(data))}
	sid := C.H5Screate_simple(1, &dims[0], nil)
	did := C.H5Dcreate2(loc, cname, C.H5T_NATIVE_INT8, sid, C.H5P_DEFAULT, C.H5P_DEFAULT, C.H5P_DEFAULT)
	C.H5Dwrite(did, C.H5T_NATIVE_INT8, C.H5S_ALL, C.H5S_ALL, C.H5P_DEFAULT, unsafe.Pointer(&data[0]))
	C.H5Dclose(did)
	C.H5Sclose(sid)
}

func writeStringAttr(obj C.hid_t, name, value string) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cval := C.CString(value)
	defer C.free(unsafe.Pointer(cval))

	sid := C.H5Screate(C.H5S_SCALAR)
	tid := C.H5Tcopy(C.H5T_C_S1)
	C.H5Tset_size(tid, C.size_t(len(value)+1))
	C.H5Tset_strpad(tid, C.H5T_STR_NULLTERM)
	aid := C.H5Acreate2(obj, cname, tid, sid, C.H5P_DEFAULT, C.H5P_DEFAULT)
	C.H5Awrite(aid, tid, unsafe.Pointer(cval))
	C.H5Aclose(aid)
	C.H5Tclose(tid)
	C.H5Sclose(sid)
}

func fatal(msg string) {
	fmt.Fprintf(os.Stderr, "mksuperblock: %s failed\n", msg)
	os.Exit(1)
}
