// Package hdf5 implements HDF5 for NetCDF
//
// The specification for HDF5 is not comprehensive and leaves out many details.
// A lot of this code was determined from reverse-engineering various HDF5
// data files. It's quite hacky for that reason.  It will get cleaned up
// in the future.
package hdf5

import (
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/batchatco/go-native-netcdf/internal"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-thrower"
)

// Magic number at the head of a valid HDF5 file
const magic = "\211HDF\r\n\032\n"

const (
	invalidAddress = ^uint64(0)
	unlimitedSize  = ^uint64(0)
)

// Datatype versions
const (
	// The doc says only early versions of the library use 1, but that's not actually
	// true and it is fairly standard.
	dtversionStandard = iota + 1 // not what the doc calls it
	dtversionArray
	dtversionPacked
	dtversionV4 // undocumented V4 datatype version
)

// For disabling/enabling code
//
// Constants for some specific things that don't seem to happen, and we don't need to unit test.
// Kept around just in case.
const (
	// The time class appears to be obsolete.
	parseTime = false
	// The same with the creation order for indexed groups; it never appears in files.
	parseCreationOrder = false
	// Enums only ever seem to be ints, though the spec hints they don't have to be.
	floatEnums = false
)

// Vars for some specific things that aren't useful or are not implemented yet,
// and so the code is disabled.
// They are vars so they can be unit tested.
var (
	// We don't implement any extended types, so we don't allow the superblock extension
	parseSBExtension = false

	// Bitfields are not part of NetCDF, but they are part of HDF5.
	allowBitfields = false

	// References are not part of NetCDF, but they are part of HDF5.
	allowReferences = false

	// Allow a few non-standard things for testing, such as ignoring non-standard headers
	allowNonStandard = false

	// We have not fully implemented V3 of the superblock.  Enabling this allows some
	// undocumented things to appear, like datatype V4, which we do not support.
	superblockV3 = false

	// We don't need to parse heap direct blocks
	parseHeapDirectBlock = false
)

// undocumented datatype version 4 is enabled with superblockV3
var maxDTVersion byte = 3

// The hidden attribute which identifies what software wrote the file out.
const ncpKey = "_NCProperties"

// Various filters on data
const (
	filterDeflate = iota + 1 // zlib
	filterShuffle
	filterFletcher32
	filterSzip        // not supported
	filterNbit        // not supported
	filterScaleOffset // not supported
)

// data types
const (
	// 0-4
	typeFixedPoint = iota
	typeFloatingPoint
	typeTime
	typeString
	typeBitField
	// 5-9
	typeOpaque
	typeCompound
	typeReference
	typeEnumerated
	typeVariableLength
	// 10
	typeArray
)

// data type names
var typeNames = []string{
	"typeFixedPoint",
	"typeFloatingPoint",
	"typeTime",
	"typeString",
	"typeBitField",
	"typeOpaque",
	"typeCompound",
	"typeReference",
	"typeEnumerated",
	"typeVariableLength",
	"typeArray",
}

// header message types
const (
	// 0-9
	typeNIL = iota
	typeDataspace
	typeLinkInfo
	typeDatatype
	typeDataStorageFillValueOld
	typeDataStorageFillValue
	typeLink
	typeExternalDataFiles
	typeDataLayout
	typeBogus
	// 10-19
	typeGroupInfo
	typeDataStorageFilterPipeline
	typeAttribute
	typeObjectComment
	typeObjectModificationTimeOld
	typeSharedMessageTable
	typeObjectHeaderContinuation
	typeSymbolTableMessage
	typeObjectModificationTime
	typeBtreeKValues
	// 20-22
	typeDriverInfo
	typeAttributeInfo
	typeObjectReferenceCount
)

// Header type to string (htts)
var htts = []string{
	// 0-9
	"NIL",
	"Dataspace",
	"Link Info",
	"Datatype",
	"Data Storage - Fill Value (Old)",
	"Data Storage - Fill Value",
	"Link",
	"External Data Files",
	"Data Layout",
	"Bogus",
	// 10-19
	"Group Info",
	"Data Storage - Filter Pipeline",
	"Attribute",
	"Object Comment",
	"Object Modification Time (Old)",
	"Shared Message Table",
	"Object Header Continuation",
	"Symbol Table Message",
	"Object Modification Time",
	"B-tree ‘K’ Values",
	// 20-22
	"Driver Info",
	"Attribute Info",
	"Object Reference Count",
}

// types of data layout classes
const (
	classCompact = iota
	classContiguous
	classChunked
	classVirtual
)

// for padBytesCheck()
const (
	dontRound = false // the number is the number of pad bytes to check for.
	round     = true  // the number is the byte-boundary to check up to (1, 3 or 7).
)

type attribute struct {
	name          string
	value         interface{}
	class         uint8
	vtType        uint8        // for variable length
	signed        bool         // for fixed-point
	children      []*attribute // for variable, compound, enums, vlen.
	enumNames     []string
	enumValues    []interface{}
	shared        bool   // if shared
	length        uint32 // datatype length
	layout        []uint64
	dimensions    []uint64 // for compound
	byteOffset    uint32   // for compound
	isSlice       bool
	firstDim      int64 // first dimension if getting slice (fake objects only)
	lastDim       int64 // last dimension if getting slice (fake objects only)
	endian        binary.ByteOrder
	dtversion     uint8
	creationOrder uint64
	df            io.Reader
	noDf          bool
}

type compoundField struct {
	Name string
	Val  interface{}
}
type compound []compoundField

type enumerated struct {
	values interface{}
}

type opaque []byte

type dataBlock struct {
	offset     uint64 // offset of data
	length     uint64 // size in bytes of data
	dsOffset   uint64 // byte offset in dataset
	dsLength   uint64 // size in byte of dataset chunk
	filterMask uint32
	offsets    []uint64
	rawData    []byte
}

type filter struct {
	kind uint16
	cdv  []uint32
}

// HDF5 implements api.Group for HDF5
type HDF5 struct {
	fname         string
	fileSize      int64
	file          *raFile
	groupName     string // fully-qualified
	rootAddr      uint64
	root          *linkInfo
	attribute     *linkInfo
	rootObject    *object
	groupObject   *object
	sharedAttrs   map[uint64]*attribute
	registrations map[string]interface{}
	addrs         map[uint64]bool
}

type linkInfo struct {
	creationIndex      uint64
	heapAddress        uint64
	btreeAddress       uint64
	creationOrderIndex uint64
	block              []uint64
	iBlock             []uint64
	heapIDLength       int
	maxHeapSize        int
	blockSize          uint64
	tableWidth         uint16
	maximumBlockSize   uint64
	rowsRootIndirect   uint16
}

type object struct {
	addr             uint64
	link             *linkInfo
	attr             *linkInfo
	children         map[string]*object
	name             string
	attrlist         []*attribute
	dataBlocks       []dataBlock
	filters          []filter
	objAttr          *attribute
	fillValue        []byte // takes precedence over old fill value
	fillValueOld     []byte
	isGroup          bool
	creationOrder    uint64
	attrListIsSorted bool
}

var (
	logFunc   = logger.Fatal // logging function for padBytesCheck()
	maybeFail = fail         // fail function, can be disabled for testing
)

// Only the pointer is used here.  We don't actually use the value.
// It's just a way to detect that something wasn't defined.
var fillValueUndefinedConstant = []byte{0xff}

var logger = internal.NewLogger()

// Prevent usage of the standard log package.
type log struct{}

var _ = log{} // to silence staticcheck warning

func setNonStandard(non bool) bool {
	old := allowNonStandard
	allowNonStandard = non
	if allowNonStandard {
		logFunc = logger.Info
		maybeFail = func(msg string) {
			logger.Warn(msg)
		}
	} else {
		logFunc = logger.Fatal
		maybeFail = fail
	}
	return old
}

// SetLogLevel sets the logging level to the given level, and returns
// the old level. This is for internal debugging use. The log messages
// are not expected to make much sense to anyone but the developers.
// The lowest level is 0 (no error logs at all) and the highest level is
// 3 (errors, warnings and debug messages).
func SetLogLevel(level int) int {
	old := logger.LogLevel()
	switch level {
	case 0:
		logger.SetLogLevel(internal.LevelFatal)
	case 1:
		logger.SetLogLevel(internal.LevelError)
	case 2:
		logger.SetLogLevel(internal.LevelWarn)
	default:
		logger.SetLogLevel(internal.LevelInfo)
	}
	return int(old)
}

func (h5 *HDF5) newSeek(addr uint64, size int64) remReader {
	logger.Infof("Seek to 0x%x", addr)
	assert(int64(addr)+size <= h5.fileSize,
		fmt.Sprintf("bad seek addr=0x%x size=%d addr+size=0x%x fileSize=0x%x",
			addr, size, int64(addr)+size, h5.fileSize))
	if size == 0 {
		size = h5.fileSize - int64(addr)
	}
	return newResetReaderOffset(h5.file, size, addr)
}

func read(r io.Reader, data interface{}) {
	err := binary.Read(r, binary.LittleEndian, data)
	thrower.ThrowIfError(err)
}

func skip(r io.Reader, length int64) {
	data := make([]byte, length)
	err := binary.Read(r, binary.LittleEndian, data)
	thrower.ThrowIfError(err)
}

func read8(r io.Reader) byte {
	var data byte
	err := binary.Read(r, binary.LittleEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func read16(r io.Reader) uint16 {
	var data uint16
	err := binary.Read(r, binary.LittleEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func read32(r io.Reader) uint32 {
	var data uint32
	err := binary.Read(r, binary.LittleEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func read64(r io.Reader) uint64 {
	var data uint64
	err := binary.Read(r, binary.LittleEndian, &data)
	thrower.ThrowIfError(err)
	return data
}

func readEnc(r io.Reader, e uint8) uint64 {
	switch e {
	case 1:
		return uint64(read8(r))
	case 2:
		return uint64(read16(r))
	case 4:
		return uint64(read32(r))
	case 8:
		return read64(r)
	default:
		fail(fmt.Sprint("bad encoded length: ", e))
	}
	panic("not reached") // silence warning
}

func (h5 *HDF5) checkChecksum(addr uint64, blen int) {
	bf := h5.newSeek(addr, int64(blen)+4) // +4 for checksum
	hash := computeChecksumStream(bf, blen)
	sum := read32(bf)
	logger.Infof("checksum 0x%x (expected 0x%x) length=%d", hash, sum, blen)
	assert(hash == sum, "checksum mismatch")
}

func computeChecksumStream(bf io.Reader, blen int) uint32 {
	ilen := blen / 4 // number of integers
	rem := blen % 4  // remaining bytes if blen is not a multiple of 4
	irem := 0
	if rem > 0 {
		irem = 1 // one extra integer if blen is not a multiple of 4
	}
	block := make([]uint32, ilen+irem)
	read(bf, block[:ilen]) // read the multiple of 4 bytes
	if irem > 0 {
		// read remaining bytes, zero-padded
		var b [4]byte
		read(bf, b[:rem])
		bff := newResetReaderFromBytes(b[:])
		// convert to integer
		block[ilen] = read32(bff)
	}
	return hashInts(block[:], uint32(blen))
}

func binaryToString(val uint64) string {
	return strconv.FormatInt(int64(val), 2)
}

func (h5 *HDF5) readSuperblock() {
	const (
		v0SBSize  = 96  // v0, 56 in superblock + 40 in symbol table
		v1SBSize  = 100 // v1,60 in superblock + 40 in symbol table
		v23SBSize = 48  // v2&v3,48 in superblock, no symbol table
	)
	sbSize := int64(v23SBSize)
	assertError(sbSize <= h5.fileSize, ErrCorrupted, "File is too small to have a superblock")

	bf := h5.newSeek(0, sbSize)

	checkMagic(bf, 8, magic)

	version := read8(bf)
	logger.Info("superblock version=", version)
	// adjust size now that we know the version
	switch version {
	case 0:
		bf = h5.newSeek(uint64(bf.Count()), v0SBSize-bf.Count())
	case 1:
		bf = h5.newSeek(uint64(bf.Count()), v1SBSize-bf.Count())
	case 2:
		break
	default:
		if !superblockV3 {
			thrower.Throw(ErrVersion)
		}
	}
	if version < 2 {
		// we've read 9 bytes of a 64 byte chunk
		b := read8(bf)
		logger.Info("Free space version=", b)

		b = read8(bf)
		logger.Info("Root group symbol table version=", b)
		checkVal(0, b, "version must always be zero")

		b = read8(bf)
		checkVal(0, b, "reserved must always be zero")

		b = read8(bf)
		logger.Info("Shared header message version", b)
		checkVal(0, b, "version must always be zero")
	}
	b := read8(bf)
	logger.Info("size of offsets=", b)
	assertError(b == 8, ErrOffsetSize, "only accept 64-bit offsets")

	b = read8(bf)
	logger.Info("size of lengths=", b)
	checkVal(8, b, "only accept 64-bit lengths")

	switch version {
	case 0, 1:
		b = read8(bf)
		checkVal(0, b, "reserved must always be zero")

		s := read16(bf)
		logger.Info("Group leaf node k", s)
		assert(s == 4, "Group leaf node K assumed to be 4 always")
		s = read16(bf)
		logger.Info("Group internal node k", s)

		flags := read32(bf)
		logger.Infof("file consistency flags=%s", binaryToString(uint64(flags)))
		if flags != 0 {
			logger.Info("flags ignored", flags)
		}
		if version == 1 {
			s := read16(bf)
			logger.Info("Indexed storage internal node k", s)
			assert(s > 0, "must be greater than zero")
			s = read16(bf)
			checkVal(0, s, "reserved must be zero")
		}
	case 2, 3:
		flags := read8(bf)
		if version == 2 && flags != 0 {
			logger.Warn("v2 ignores flags", flags)
		}
		logger.Infof("file consistency flags=%s", binaryToString(uint64(flags)))
	}

	baseAddress := read64(bf)
	logger.Info("base address=", baseAddress)
	checkVal(0, baseAddress, "only support base address of zero")

	sbExtension := invalidAddress
	switch version {
	case 0, 1:
		fsIndexAddr := read64(bf)
		logger.Infof("free-space index address=%x", fsIndexAddr)
		checkVal(invalidAddress, fsIndexAddr, "free-space index address not supported")
	case 2, 3:
		sbExtension = read64(bf)
		logger.Infof("superblock extension address=%x", sbExtension)
	}

	eofAddr := read64(bf)
	logger.Infof("end of file address=0x%x", eofAddr)
	assertError(eofAddr <= uint64(h5.fileSize),
		ErrTruncated,
		fmt.Sprint("File may be truncated. size=", h5.fileSize, " expected=", eofAddr))

	infoAssert(uint64(h5.fileSize) == eofAddr,
		fmt.Sprint("Junk at end of file ignored. size=", h5.fileSize, " expected=", eofAddr))

	switch version {
	case 0, 1:
		driverInfoAddress := read64(bf)
		logger.Infof("driver info address=0x%x", driverInfoAddress)

		// get the root address
		linkNameOffset := read64(bf) // link name offset
		objectHeaderAddress := read64(bf)
		logger.Infof("Root group STE link name offset=%d header addr=0x%x",
			linkNameOffset, objectHeaderAddress)
		cacheType := read32(bf)
		logger.Info("cacheType", cacheType)
		reserved := read32(bf)
		checkVal(0, reserved, "reserved sb")
		if cacheType == 1 {
			btreeAddr := read64(bf)
			heapAddr := read64(bf)
			logger.Infof("btree addr=0x%x heap addr=0x%x", btreeAddr, heapAddr)
		}
		h5.rootAddr = objectHeaderAddress
	case 2, 3:
		rootAddr := read64(bf)
		logger.Infof("root group object header address=%d", rootAddr)
		h5.rootAddr = rootAddr
		h5.checkChecksum(0, 44)
	}
	if sbExtension != invalidAddress {
		if parseSBExtension {
			logger.Warn("parsing unsupported superblock extension")
			obj := newObject()
			h5.readDataObjectHeader(obj, sbExtension)
		} else {
			logger.Warn("superblock extension not supported")
			thrower.Throw(ErrSuperblock)
		}
	}
}

func checkMagic(bf io.Reader, len int, magic string) {
	b := make([]byte, len)
	read(bf, b)
	found := string(b)
	if found != magic {
		logger.Infof("bad magic=%q expected=%q", found, magic)
		thrower.Throw(ErrBadMagic)
	}
}

func getString(b []byte) string {
	end := 0
	for i := range b {
		if b[i] == 0 {
			break
		}
		end++
	}
	return string(b[:end])
}

func readNullTerminatedName(bf io.Reader, padding int) string {
	var name []byte
	nullFound := false
	for bf.(remReader).Rem() > 0 && !nullFound {
		b := read8(bf)
		if b == 0 {
			logger.Info("namelen=", len(name))
			nullFound = true
			break
		}
		name = append(name, b)
	}
	if !nullFound {
		logger.Warn("short string", string(name))
		thrower.Throw(io.EOF)
	}
	if padding > 0 {
		// remove pad
		namelenplus := len(name) + 1
		logger.Info("namelenplus", namelenplus)
		namelenpadded := (namelenplus + padding) & ^padding
		logger.Info("namelenpadded", namelenpadded)
		extra := namelenpadded - namelenplus
		logger.Info("pad", extra)
		if extra > 0 {
			checkZeroes(bf, extra)
		}
	}
	return string(name)
}

func padBytes(bf io.Reader, pad32 int) {
	padBytesCheck(bf, pad32, round, logFunc)
}

func checkZeroes(bf io.Reader, len int) {
	padBytesCheck(bf, len, dontRound, logFunc)
}

// Assumes it is an attribute
func (h5 *HDF5) readAttributeDirect(obj *object, addr uint64, offset uint64, length uint16,
	creationOrder uint64) {
	logger.Infof("* addr=0x%x offset=0x%x length=%d", addr, offset, length)
	logger.Info("read Attributes at:", addr+offset)
	bf := h5.newSeek(addr+uint64(offset), int64(length))
	h5.readAttribute(obj, bf, creationOrder)
}

func printDatatype(hr heapReader, c caster, bf remReader, df remReader, objCount int64, attr *attribute) {
	assert(bf.Rem() >= 8, "short data")
	b0 := read8(bf)
	b1 := read8(bf)
	b2 := read8(bf)
	b3 := read8(bf)
	bitFields := uint32(b1) | (uint32(b2) << 8) | (uint32(b3) << 16)
	dtversion := (b0 >> 4) & 0b1111
	dtclass := b0 & 0b1111
	dtlength := read32(bf)
	logger.Infof("* length=%d dtlength=%d dtversion=%d class=%s flags=%s",
		bf.Rem(), dtlength,
		dtversion, typeNames[dtclass], binaryToString(uint64(bitFields)))
	switch dtversion {
	case dtversionStandard:
		logger.Info("Standard datatype")
	case dtversionArray:
		logger.Info("Array-encoded datatype")
	case dtversionPacked:
		logger.Info("VAX and/or packed datatype")
	case dtversionV4:
		if maxDTVersion == dtversionV4 {
			// allowed
			logger.Info("Undocumented datatype version 4")
			break
		}
		fallthrough
	default:
		fail(fmt.Sprint("Unknown datatype version: ", dtversion))
	}
	attr.dtversion = dtversion
	attr.class = dtclass
	attr.length = dtlength
	assert(attr.length != 0, "attr length can't be zero")
	parse(dtclass, hr, c, attr, bitFields, bf, df)
	if df != nil && df.Rem() > 0 {
		// It is normal for there to be extra data, not sure why yet.
		// It does not break any unit tests, so the extra data seems unnecessary.
		logger.Info("did not read all data", df.Rem(), typeNames[dtclass])
		skip(df, df.Rem())
	}
}

func (h5 *HDF5) readAttribute(obj *object, obf io.Reader, creationOrder uint64) {
	bf := obf.(remReader)
	logger.Info("size=", bf.Rem())
	version := read8(bf)
	logger.Infof("* attr version=%d", version)
	assert(version >= 1 && version <= 3, "not an Attribute")
	flags := read8(bf) // reserved in version 1
	sharedType := false
	sharedSpace := false
	switch version {
	case 1:
		checkVal(0, flags, "reserved field must be zero")
	case 2, 3:
		if hasFlag8(flags, 0) {
			logger.Info("shared datatype")
			sharedType = true
		}
		if hasFlag8(flags, 1) {
			// This flag never seems to be set
			logger.Info("shared dataspace")
			sharedSpace = true
		}
		logger.Infof("* attr flags=0x%x (%s)", flags, binaryToString(uint64(flags)))
	}
	nameSize := read16(bf)
	logger.Infof("* name size: %d", nameSize)
	datatypeSize := read16(bf)
	logger.Infof("* datatype size: %d", datatypeSize)
	dataspaceSize := read16(bf)
	logger.Infof("* dataspace size: %d", dataspaceSize)
	if version == 3 {
		enc := read8(bf)
		logger.Infof("* encoding: %d", enc)
	}
	assert(nameSize > 0, "bad name size")
	b := make([]byte, nameSize)
	read(bf, b)
	if version == 1 {
		padBytes(bf, 7)
	}
	// save name
	name := getString(b)
	logger.Infof("* attribute name=%s", string(b[:nameSize-1]))
	attr := &attribute{name: name}
	logger.Infof("* attribute ptr=%p", attr)
	attr.creationOrder = creationOrder
	dtb := make([]byte, datatypeSize)
	read(bf, dtb)
	logger.Infof("** orig datatype=0x%x", dtb)

	if version == 1 {
		pad := ((datatypeSize + 7) & ^uint16(7)) - datatypeSize
		logger.Info("datatype pad", datatypeSize, pad)
		for i := 0; i < int(pad); i++ {
			z := read8(bf)
			checkVal(0, z, "zero pad")
		}
	}
	var dims []uint64
	var count int64
	if sharedSpace {
		// This flag does't seem to ever get set.
		checkVal(datatypeSize, 10, "datatype size must be 10 for shared")
		sVersion := read8(bf)
		sType := read8(bf)
		addr := read64(bf)
		logger.Infof("shared space version=%v type=%v addr=%x", sVersion, sType, addr)
		fail("don't handle shared dataspaces")
	} else {
		dims, count = h5.readDataspace(newResetReader(bf, int64(dataspaceSize)))
		if version == 1 {
			padBytes(bf, 7)
		}
		logger.Info("dimensions were", attr.dimensions)
		attr.dimensions = dims
		logger.Info("dimensions are", dims)
		logger.Info("count objects=", count)
	}
	logger.Info("sizeRem=", bf.Rem())
	if !sharedType {
		pf := newResetReaderFromBytes(dtb)
		printDatatype(h5, h5, pf, bf, count, attr)
	} else {
		checkVal(datatypeSize, 10, "datatype size must be 10 for shared")
		bff := newResetReaderFromBytes(dtb)
		sVersion := read8(bff)
		sType := read8(bff)
		logger.Infof("shared type version=%v type=%v", sVersion, sType)
		switch sVersion {
		case 0, 1: // 0 is also version 1

			// Warn because this version has never been seen by this code
			logger.Warn("version 1 shared message encountered")
			checkVal(sType, 0, "type must be zero")
			checkZeroes(bff, 6)
		case 2:
			// the type is supposed to be zero for version 2, but is sometimes 2
			assert(sType == 0 || sType == 2, "type must be 0 or 2")
		case 3:
			// Warn because this version has never been seen by this code
			// The code here may not be correct.
			logger.Warn("version 3 shared message encountered")
			switch sType {
			case 0:
				logger.Info("not actually shared")
			case 1:
				logger.Info("message in heap")
			case 2:
				logger.Info("messsage in an object")
			case 3:
				logger.Info("message not shared, but sharable")
			default:
				fail("Unimplemented shared message feature")
			}
		}
		addr := read64(bff)
		logger.Infof("shared type addr=0x%x", addr)
		oa := h5.getSharedAttr(obj, addr)
		oa.dimensions = dims
		oa.name = name
		attr = oa
		if bf.Rem() > 0 {
			oa.df = newResetReaderSave(bf, bf.Rem())
		}
	}
	obj.attrlist = append(obj.attrlist, attr)
}

func (h5 *HDF5) getSharedAttr(obj *object, addr uint64) *attribute {
	oa := h5.sharedAttrs[addr]
	if oa == nil {
		oa = obj.objAttr
		h5.sharedAttrs[addr] = oa
		h5.readDataObjectHeader(obj, addr)
		oa.shared = true
		if oa.df != nil {
			oa.noDf = true // don't reparse
		}
		return oa
	}
	if oa.df == nil && !oa.noDf {
		oa.noDf = true // avoids loop
		h5.readDataObjectHeader(obj, addr)
		oa.noDf = true // avoids loop
	}
	return oa
}

type doublerCallback func(obj *object, bnum uint64, offset uint64, length uint16,
	creationOrder uint64)

// Handling doubling table.  Assume width of 4.
func (h5 *HDF5) doDoubling(obj *object, link *linkInfo, offset uint64, length uint16, creationOrder uint64, callback doublerCallback) {
	logger.Infof("doubling start: offset=0x%x length=%d blocksize=%d block=%x iblock=%x", offset, length, link.blockSize, link.block, link.iBlock)
	blockSize := link.blockSize
	blockToUse := invalidAddress
	// First try direct blocks
	width := int(link.tableWidth)
	for entryNum, block := range link.block {
		if offset < blockSize && block != invalidAddress {
			blockToUse = block
			break
		}
		offset -= blockSize
		row := entryNum / width
		// We double the third row and beyond, up to the maximum.
		// test is if we are at the end of a row, then we may double.
		if row >= 1 && (entryNum%width) == (width-1) && blockSize < link.maximumBlockSize {
			logger.Info("doubled block size", blockSize, "->", blockSize*2, "row=", row,
				"max=", link.maximumBlockSize)
			blockSize *= 2
		}
	}
	if offset < blockSize && blockToUse != invalidAddress {
		callback(obj, blockToUse, offset, length, creationOrder)
		return
	}

	// now try indirect blocks
	logger.Infof("Using indirect blocks offset=0x%x", offset)
	blockSize *= 2
	// blockSize = link.blockSize
	for entryNum, block := range link.iBlock {
		logger.Infof("Trying block 0x%x offset=0x%x", block, offset)
		if offset < blockSize && block != invalidAddress {
			logger.Infof("Found indirect block 0x%x offset=0x%x", block, offset)
			blockToUse = block
			break
		}
		offset -= blockSize
		if (entryNum % width) == (width - 1) {
			logger.Warn("INDIRECT doubled block size", blockSize, "->", blockSize*2,
				"max=", link.maximumBlockSize)
			blockSize *= 2
		}
	}
	assert(blockToUse != invalidAddress, "did not find direct or indirect block")

	nextLink := *link

	logger.Infof("Read indirect block 0x%x %d", blockToUse, blockSize)
	nrows := log2(blockSize) - log2(link.blockSize*uint64(width)) + 1
	logger.Info("calculated rows=", nrows, "blocksize=", blockSize)
	h5.readRootBlock(&nextLink, blockToUse, 0, uint16(nrows))

	h5.readLinkData(obj, &nextLink, offset, length, creationOrder, callback)
}

func (h5 *HDF5) readLinkData(obj *object, link *linkInfo, offset uint64, length uint16,
	creationOrder uint64, callback doublerCallback) {
	logger.Infof("offset=0x%x length=%d", offset, length)
	h5.doDoubling(obj, link, offset, length, creationOrder, callback)
}

func hasFlag8(flags byte, flag uint) bool {
	return (flags>>flag)&1 == 1
}

// Assumes it is a link
func (h5 *HDF5) readLinkDirect(parent *object, addr uint64, offset uint64, length uint16,
	creationOrder uint64) {
	logger.Infof("* addr=0x%x offset=0x%x length=%d", addr, offset, length)
	bf := h5.newSeek(addr+uint64(offset), int64(length))
	h5.readLinkDirectFrom(parent, bf, length, creationOrder)
}

func (h5 *HDF5) readLinkDirectFrom(parent *object, obf io.Reader, length uint16, creationOrder uint64) {
	bf := newResetReader(obf, int64(length))
	version := read8(bf)
	logger.Infof("* link version=%d", version)
	checkVal(1, version, "Link version must be 1")
	flags := read8(bf)
	logger.Infof("* link flags=0x%x (%s)", flags, binaryToString(uint64(flags)))
	linkType := byte(0)
	if hasFlag8(flags, 3) {
		linkType = read8(bf)
		logger.Info("linkType=", linkType)
	}
	var co uint64
	if hasFlag8(flags, 2) {
		co = read64(bf)
		logger.Info("co=", co, "creationOrder=", creationOrder)
	}
	hasCSet := hasFlag8(flags, 4)
	if hasCSet {
		// This flag never seems to be set
		cSet := read8(bf)
		logger.Info("cset=", cSet)
		assert(cSet == 0 || cSet == 1, "only ASCII and UTF-8 names")
	}
	size := 1 << (flags & 0b11)
	b := readEnc(bf, uint8(size))
	lenlen := uint64(b)
	logger.Infof("lenlen=0x%x", lenlen)
	linkName := make([]byte, lenlen)
	if lenlen > 0 {
		read(bf, linkName)
	}
	logger.Infof("start with link name=%s lenlen=%d", string(linkName), lenlen)
	logger.Info("remlen=", bf.Rem())
	if linkType != 0 {
		switch linkType {
		case 1:
			logger.Error("soft links not supported")
		case 64:
			logger.Error("external links not supported")
		default:
			logger.Error("unsupported link type", linkType)
		}
		thrower.Throw(ErrLinkType)
	}
	hardAddr := read64(bf)
	if bf.Rem() > 0 {
		checkZeroes(bf, int(bf.Rem()))
	}
	logger.Infof("hard link=0x%x", hardAddr)
	_, has := parent.children[string(linkName)]
	assert(!has, "duplicate object")
	if h5.hasAddr(hardAddr) {
		logger.Info("avoid link loop")
		logger.Infof("done with name=%s", string(linkName))
		return
	}
	obj := newObject()
	obj.name = string(linkName)
	parent.children[obj.name] = obj
	obj.creationOrder = co
	obj.addr = hardAddr
	h5.addrs[hardAddr] = true
	h5.readDataObjectHeader(obj, hardAddr)
	logger.Info("obj name", obj.name)
	logger.Infof("object (0x%x, %s) from parent (0x%x, %s)\n",
		obj.addr, obj.name, parent.addr, parent.name)
	h5.dumpObject(obj)
	logger.Infof("done with name=%s", string(linkName))
}

func (h5 *HDF5) readBTreeInternal(parent *object, bta uint64, numRec uint64, recordSize uint16, depth uint16, nodeSize uint32) {
	nr := uint64(numRec) // should work
	len := 4 + 2 + nr*uint64(recordSize)
	sub := 0
	if depth > 1 {
		sub = 2
	}
	bsize := int64(4+2) + int64(nr)*int64(recordSize) + (int64(nr)+1)*int64(8+1+sub)
	bf := h5.newSeek(bta, bsize)
	checkMagic(bf, 4, "BTIN")
	version := read8(bf)
	checkVal(0, version, "version")
	logger.Info("btin version=", version)
	ty := read8(bf)
	logger.Info("btin type=", ty)
	logger.Info("btin numrec=", numRec)
	logger.Info("btin recordSize=", recordSize)
	logger.Info("nr=", nr)
	logger.Info("depth = ", depth)
	logger.Info("count before=", bf.Count())
	h5.readRecords(parent, bf, nr, ty)
	logger.Info("count after=", bf.Count())

	for i := uint64(0); i <= nr; i++ {
		cnp := read64(bf) // child node pointer
		len += 8
		logger.Infof("cnp=0x%x", cnp)
		// not sure this calculation is right
		fixedSizeOverhead := uint32(10)
		onePointerTriplet := uint32(16)
		maxNumberOfRecords := uint64(nodeSize-(fixedSizeOverhead+onePointerTriplet)) / (uint64(recordSize) + uint64(onePointerTriplet))
		logger.Info("max number of records", maxNumberOfRecords)
		assert(maxNumberOfRecords < 256, "can't handle this") // TODO: support bigger maxes
		cnr := read8(bf)                                      // number of records for child node
		len++
		logger.Infof("cnr=0x%x", cnr)
		logger.Info("depth=", depth)
		if depth == 1 {
			logger.Info("Descend into leaf")
			h5.readBTreeLeaf(parent, cnp, uint64(cnr), recordSize)
		} else {
			logger.Info("Descend into node")
			h5.readBTreeInternal(parent, cnp, uint64(cnr), recordSize, depth-1, nodeSize)
		}
		if depth > 1 {
			tnr := read16(bf) // total number of records in child node
			len += 2
			logger.Infof("tnr=0x%x", tnr)
		}
	}
	checkVal(int64(len), bsize, "accounting problem")
	h5.checkChecksum(bta, int(len))
}

func (h5 *HDF5) readRecords(obj *object, bf io.Reader, numRec uint64, ty byte) {
	logger.Info("ty=", ty)
	for i := 0; i < int(numRec); i++ {
		logger.Infof("reading record %d of %d", i, numRec)
		switch ty {
		case 5: // for indexing the ‘name’ field for links in indexed groups.
			logger.Info("Name field for links in indexed groups")
			hash := read32(bf)
			// heap ID
			versionAndType := read8(bf)
			logger.Infof("hash=0x%x versionAndType=%s", hash,
				binaryToString(uint64(versionAndType)))
			idType := (versionAndType >> 4) & 0b11
			checkVal(0, idType, "don't know how to handle non-managed")
			logger.Info("idtype=", idType)
			// heap IDs are always 7 bytes here
			offset := uint64(read32(bf))
			length := read16(bf)
			// done reading heap id
			logger.Infof("offset=0x%x length=%d", offset, length)
			logger.Info("read link data -- indexed groups")
			h5.readLinkData(obj, obj.link, offset, length, 0, h5.readLinkDirect)
		case 6: // creation order for indexed group
			if parseCreationOrder {
				logger.Info("Creation order for indexed groups")
				co := read64(bf)
				versionAndType := read8(bf)
				logger.Infof("co=0x%x versionAndType=0x%x", co, versionAndType)
				idType := (versionAndType >> 4) & 0b11
				checkVal(0, idType, "don't know how to handle non-managed")
				// heap IDs are always 8 bytes here
				offset := uint64(read32(bf))
				length := read16(bf)
				// done reading heap id
				logger.Infof("offset=0x%x length=%d", offset, length)
				// XXX: TODO: don't downcast creationOrder
				h5.readLinkData(obj, obj.link, offset, length, co, h5.readLinkDirect)
			} else {
				logger.Fatal("creation order code has never been executed before")
			}

		case 8: // for indexing the ‘name’ field for indexed attributes.
			logger.Info("Name field for indexed attributes")
			versionAndType := read8(bf)
			logger.Infof("versionAndType=%s", binaryToString(uint64(versionAndType)))
			idType := (versionAndType >> 4) & 0b11
			logger.Info("idtype=", idType)
			checkVal(0, idType, "don't know how to handle non-managed")
			// heap IDs are always 8 bytes here
			offset := uint64(read32(bf))
			logger.Infof("offset=0x%x", offset)
			more := read8(bf)
			logger.Infof("more=0x%x", more)
			offset = offset | uint64(more)<<32
			length := read16(bf)
			// done reading heap id
			flags := read8(bf)
			co := read32(bf)
			hash := read32(bf)
			logger.Infof("flags=%s co=0x%x hash=0x%x",
				binaryToString(uint64(flags)), co, hash)
			logger.Info("read link data -- indexed attributes")
			h5.readLinkData(obj, obj.attr, offset, length, uint64(co), h5.readAttributeDirect)

		case 9:
			// uncomment the following to enable
			if parseCreationOrder {
				logger.Info("Creation order for indexed attributes")
				// byte 1 of heap id
				versionAndType := read8(bf)
				logger.Infof("versionAndType=%s", binaryToString(uint64(versionAndType)))
				idType := (versionAndType >> 4) & 0b11
				logger.Info("idtype=", idType)
				checkVal(0, idType, "don't know how to handle non-managed")
				// heap IDs are always 8 bytes here
				// bytes 2,3,4,5 of heap id
				offset := uint64(read32(bf))
				// byte 6 of heap ID
				more := read8(bf)
				offset = offset | uint64(more)<<32
				// bytes 7 and 8 and heap ID
				length := read16(bf)
				// done reading heap id
				mflags := read8(bf)
				co := read32(bf)
				logger.Infof("type 9 vat=0x%x offset=0x%x length=%d mflags=0x%x, co=%d",
					versionAndType,
					offset, length, mflags, co)
				h5.readLinkData(obj, obj.attr, offset, length, 0, h5.readAttributeDirect)
			} else {
				logger.Fatal("creation order code has never been executed before")
			}
		default:
			fail(fmt.Sprintf("unhandled type: %d", ty))
		}
	}
}

func (h5 *HDF5) readBTreeLeaf(parent *object, bta uint64, numRec uint64, recordSize uint16) {
	nbytes := 4 + 2 + int(numRec)*int(recordSize)
	bf := h5.newSeek(bta, int64(nbytes))
	checkMagic(bf, 4, "BTLF")
	version := read8(bf)
	logger.Info("btlf version=", version)
	ty := read8(bf)
	logger.Info("bt type=", ty)
	h5.readRecords(parent, bf, numRec, ty)
	logger.Infof("leaf node size=%d", nbytes)
	h5.checkChecksum(bta, nbytes)
}

func (h5 *HDF5) readBTreeNode(parent *object, bta uint64, dtSize uint64,
	numberOfElements uint64, dimensionality uint8) {
	offset := h5.readBTreeNodeAny(parent, bta, true /*isTop*/, dtSize, numberOfElements, 0,
		dimensionality)
	logger.Info("DS offset", offset)
}

func (h5 *HDF5) readBTreeNodeAny(parent *object, bta uint64, isTop bool,
	dtSize uint64, numberOfElements uint64, dsOffset uint64, dimensionality uint8) uint64 {
	bf := h5.newSeek(bta, 24) // adjust later
	checkMagic(bf, 4, "TREE")
	logger.Infof("readBTreeNode addr 0x%x dtSize %d\n", bta, dtSize)
	nodeType := read8(bf)
	checkVal(1, nodeType, "raw data only")
	nodeLevel := read8(bf)
	entriesUsed := read16(bf)
	leftAddress := read64(bf)
	rightAddress := read64(bf)
	logger.Infof("dim=%d nodeSize=%v type=%v level=%v entries=%v left=0x%x right=0x%x",
		dimensionality,
		dtSize,
		nodeType, nodeLevel, entriesUsed, leftAddress, rightAddress)
	if leftAddress != invalidAddress || rightAddress != invalidAddress {
		assert(!isTop, "Siblings unexpected")
	}
	if nodeLevel > 0 {
		logger.Infof("Start level %d", nodeLevel)
	}
	dimDataSize := 0
	if dimensionality > 0 {
		dimDataSize = 8 * int(dimensionality-1)
	}
	bf = h5.newSeek(bta+uint64(bf.Count()),
		int64((int(entriesUsed)*(24+dimDataSize))+16+dimDataSize))
	for i := uint16(0); i < entriesUsed; i++ {
		sizeChunk := read32(bf)
		filterMask := read32(bf)
		if nodeLevel == 0 {
			logger.Infof("[%d] sizeChunk=%d filterMask=0x%x", i, sizeChunk, filterMask)
		}
		offsets := make([]uint64, dimensionality-1)
		for d := uint8(0); d < dimensionality-1; d++ {
			offset := read64(bf)
			offsets[d] = offset
			if nodeLevel == 0 {
				logger.Infof("[%d] dim offset %d/%d: 0x%08x (%d)", i, d, dimensionality, offset,
					offset)
			}
		}
		offset := read64(bf)
		if nodeLevel == 0 {
			logger.Infof("[%d] dim offset final/%d: 0x%08x (%d)", i, dimensionality, offset,
				offset)
		}
		checkVal(0, offset, "last offset must be zero")
		addr := read64(bf)

		if nodeLevel == 0 {
			logger.Infof("[%d] addr: 0x%x, %d", i, addr, sizeChunk)
		}
		if nodeLevel > 0 {
			logger.Infof("read middle: 0x%x, %d", addr, nodeLevel)
			dsOffset = h5.readBTreeNodeAny(parent, addr, false /*not top*/, dtSize,
				numberOfElements, dsOffset, dimensionality)
			continue
		}
		dso := uint64(0)
		sizes := uint64(dtSize)
		if parent.objAttr.dimensions != nil {
			for d := int(dimensionality) - 2; d >= 0; d-- {
				dso += offsets[d] * sizes
				logger.Info("d=", d, "dim=", dimensionality, "parent dim=", parent.objAttr.dimensions)
				sizes *= parent.objAttr.dimensions[d]
			}
		}
		pending := dataBlock{addr, uint64(sizeChunk), 0, 0, filterMask, nil, nil}
		pending.dsOffset = dso
		pending.dsLength = numberOfElements * dtSize
		pending.offsets = offsets
		logger.Info("dsoffset", dso, "dslength", pending.dsLength, "dtsize", dtSize)
		parent.dataBlocks = append(parent.dataBlocks, pending)
		dsOffset += pending.dsLength
	}
	if nodeLevel > 0 {
		logger.Infof("Done level %d", nodeLevel)
		return dsOffset
	}
	finalSizeChunk := read32(bf)
	filterMask := read32(bf)
	logger.Infof("[final] sizeChunk=%d filterMask=0x%x", finalSizeChunk, filterMask)
	for d := uint8(0); d < dimensionality-1; d++ {
		offset := read64(bf)
		logger.Infof("[final] dim offset %d/%d: 0x%08x (%d)", d, dimensionality, offset,
			offset)
	}
	offset := read64(bf)
	logger.Infof("[final] dim offset final/%d: 0x%08x (%d)", dimensionality, offset,
		offset)
	return dsOffset
}

func (h5 *HDF5) readHeapDirectBlock(link *linkInfo, addr uint64, flags uint8,
	blockSize uint64) {
	if parseHeapDirectBlock { // we don't need this code
		logger.Infof("heap direct block=0x%x size=%d", addr, blockSize)
		bf := h5.newSeek(addr, int64(blockSize))
		checkMagic(bf, 4, "FHDB")
		version := read8(bf)
		logger.Info("heap direct version=", version)
		checkVal(0, version, "version")
		heapHeaderAddr := read64(bf)
		logger.Infof("heap header addr=0x%x", heapHeaderAddr)
		blockOffset := uint64(read32(bf))
		checksumOffset := 13 + (link.maxHeapSize / 8)
		logger.Info("maxheapsize", link.maxHeapSize)
		if link.maxHeapSize == 40 {
			logger.Info("1 more byte")
			more := read8(bf)
			blockOffset = blockOffset | (uint64(more) << 32)
		}
		logger.Infof("block offset=0x%x", blockOffset)
		logger.Infof("(block size=%d)", blockSize)
		logger.Info("flags", flags)
		if !hasFlag8(flags, 1) {
			logger.Info("Do not check checksum")
			return
		}
		checksum := read32(bf)
		bf = h5.newSeek(addr, int64(blockSize))
		// Zero out pre-existing checksum field and recalculate
		b := make([]byte, blockSize)
		read(bf, b)
		for i := 0; i < 4; i++ {
			b[checksumOffset+i] = 0
		}
		bff := newResetReaderFromBytes(b)
		hash := computeChecksumStream(bff, int(blockSize))
		logger.Infof("checksum=0x%x (expect=0x%x)", hash, checksum)
		assert(checksum == hash, "checksum mismatch")
	}
}

func log2(v uint64) int {
	r := -1
	for v > 0 {
		r++
		v >>= 1
	}
	return r
}

func (h5 *HDF5) readRootBlock(link *linkInfo, bta uint64, flags uint8, nrows uint16) {
	width := link.tableWidth
	startBlockSize := link.blockSize
	maxBlockSize := link.maximumBlockSize
	// bytes in block
	// signature=4 version=1 heapaddr=8 blockoffset=(calc) + variables=(calc) + checksum
	bSize := 4 + 1 + 8 + int64(link.maxHeapSize/8) + int64(nrows*width*8) + 4
	bf := h5.newSeek(bta, bSize)
	checkMagic(bf, 4, "FHIB")
	version := read8(bf)
	logger.Info("heap root block version=", version)
	checkVal(0, version, "heap root block version must be zero")
	heapHeaderAddr := read64(bf)
	logger.Infof("heap header addr=0x%x", heapHeaderAddr)
	blockOffset := uint64(read32(bf))
	logger.Infof("block offset=0x%x", blockOffset)
	logger.Info("max heap size", link.maxHeapSize)
	if link.maxHeapSize == 40 {
		logger.Info("1 more byte")
		more := read8(bf)
		blockOffset = blockOffset | (uint64(more) << 32)
		logger.Infof("new block offset=0x%x", blockOffset)
	}
	logger.Info("rows width=", nrows, width)
	maxRowsDirect := log2(maxBlockSize) - log2(startBlockSize) + 2
	directRows := maxRowsDirect
	indirectRows := 0
	if nrows < uint16(maxRowsDirect) {
		directRows = int(nrows)
	} else {
		indirectRows = int(nrows) - maxRowsDirect
	}
	logger.Infof("maxrowsdirect=%d directRows=%d indirectRows=%d",
		maxRowsDirect, directRows, indirectRows)

	addrs := make([]uint64, 0, directRows*int(width))
	iAddrs := make([]uint64, 0, indirectRows*int(width))
	blockSize := startBlockSize
	for i := 0; i < int(nrows); i++ {
		if i > 1 {
			if i < maxRowsDirect {
				if blockSize < maxBlockSize {
					blockSize *= 2
					logger.Info("doubled block size (direct)", blockSize, "->", blockSize*2)
				}
			} else if i >= maxRowsDirect {
				blockSize *= 2
				logger.Info("doubled block size (indirect)", blockSize, "->", blockSize*2)
			}
		}
		for j := 0; j < int(width); j++ {
			childDirectBlockAddress := read64(bf)
			logger.Infof("child block address=0x%x row=%d maxrows=%d", childDirectBlockAddress,
				i, maxRowsDirect)
			if i < maxRowsDirect {
				addrs = append(addrs, childDirectBlockAddress)
			} else {
				iAddrs = append(iAddrs, childDirectBlockAddress)
			}
		}
	}
	link.block = addrs   // direct blocks
	link.iBlock = iAddrs // indirect blocks
	h5.checkChecksum(bta, int(bSize)-4)
}

func checkVal(expected, actual interface{}, comment string) {
	extractVal := func(generic interface{}) (uint64, bool) {
		var val uint64
		switch v := generic.(type) {
		case uint64:
			val = uint64(v)
		case uint32:
			val = uint64(v)
		case uint16:
			val = uint64(v)
		case uint8:
			val = uint64(v)
		case int64:
			val = uint64(v)
		case int32:
			val = uint64(int32(v))
		case int16:
			val = uint64(v)
		case int8:
			val = uint64(v)
		case int:
			val = uint64(v)
		default:
			return 0, true
		}
		return val, false
	}
	eInt, eUnset := extractVal(expected)
	aInt, aUnset := extractVal(actual)
	match := false
	if aUnset && eUnset {
		match = expected == actual
	}
	if !aUnset && !eUnset {
		match = aInt == eInt
	}
	assert(match,
		fmt.Sprintf("expected %v != actual %v (%v)", expected, actual, comment))
}

func (h5 *HDF5) readGlobalHeap(heapAddress uint64, index uint32) (remReader, uint64) {
	bf := h5.newSeek(heapAddress, 16) // adjust size later
	checkMagic(bf, 4, "GCOL")
	version := read8(bf)
	checkVal(1, version, "version")
	checkZeroes(bf, 3)
	csize := read64(bf) // collection size, including these fields
	csize -= 16
	bf = h5.newSeek(heapAddress+uint64(bf.Count()), int64(csize))
	for csize >= 16 {
		hoi := read16(bf) // heap object index
		rc := read16(bf)  // reference count
		checkVal(0, rc, "refcount")
		zero := read32(bf) // reserved
		checkVal(0, zero, "zero")
		osize := read64(bf) // object size)
		csize -= 16
		assert(osize <= csize, "object size invalid")
		if osize > 0 {
			// adjust size
			// round up to 8-byte boundary
			asize := (osize + 7) & ^uint64(0x7)
			assert(asize <= csize, "adjusted size too big")
			csize -= asize
			if hoi == uint16(index) {
				return newResetReader(bf, int64(osize)), osize
			}
			skip(bf, int64(osize))
			l := osize
			if l > 8 {
				l = 8
			}
			rem := asize - osize
			skip(bf, int64(rem))
		}
	}
	return nil, 0
}

func (h5 *HDF5) readHeap(link *linkInfo) {
	bf := h5.newSeek(link.heapAddress, 144)
	checkMagic(bf, 4, "FRHP")
	version := read8(bf)
	logger.Info("fractal heap version=", version)
	heapIDLen := read16(bf)
	link.heapIDLength = int(heapIDLen)
	logger.Info("heap ID length=", heapIDLen)
	filterLen := read16(bf)
	logger.Info("filter length=", filterLen)
	checkVal(0, filterLen, "filterlen must be zero")
	flags := read8(bf)
	logger.Infof("flags=%s", binaryToString(uint64(flags)))
	if !hasFlag8(flags, 1) {
		logger.Warn("not using checksums")
	}
	maxSizeObjects := read32(bf)
	logger.Infof("maxSizeManagedObjects=%d", maxSizeObjects)
	nextHuge := read64(bf)
	logger.Infof("nextHuge=0x%x", nextHuge)
	btAddr := read64(bf)
	logger.Infof("btree address=0x%x", btAddr)
	amountFree := read64(bf)
	logger.Infof("amount free=%d", amountFree)
	freeSpaceAddr := read64(bf)
	logger.Infof("free space address=0x%x", freeSpaceAddr)
	amountManaged := read64(bf)
	logger.Infof("amount managed=%d", amountManaged)
	amountAllocated := read64(bf)
	logger.Infof("amount allocated=%d", amountAllocated)
	directBlockOffset := read64(bf)
	logger.Infof("direct block offset=0x%x", directBlockOffset)
	numberManaged := read64(bf)
	logger.Infof("number managed object=%d", numberManaged)
	sizeHugeObjects := read64(bf)
	logger.Infof("size huge objects=%d", sizeHugeObjects)
	numberHuge := read64(bf)
	logger.Infof("number huge objects=%d", numberHuge)
	sizeTinyObjects := read64(bf)
	logger.Infof("size tiny objects=%d", sizeTinyObjects)
	numberTiny := read64(bf)
	logger.Infof("number tiny objects=%d", numberTiny)
	tableWidth := read16(bf)
	logger.Infof("table width=%d", tableWidth)
	checkVal(4, tableWidth, "table width must be 4")
	link.tableWidth = tableWidth
	startingBlockSize := read64(bf)
	link.blockSize = startingBlockSize
	logger.Infof("starting block size=%d", startingBlockSize)
	maximumBlockSize := read64(bf)
	logger.Infof("maximum direct block size=%d", maximumBlockSize)
	link.maximumBlockSize = maximumBlockSize
	maximumHeapSize := read16(bf)
	logger.Infof("maximum heap size=%d", maximumHeapSize)
	assert(maximumHeapSize == 32 || maximumHeapSize == 40, "unhandled heap size")
	link.maxHeapSize = int(maximumHeapSize)
	startingNumberRows := read16(bf)
	logger.Infof("starting number rows=%d", startingNumberRows)
	rootBlockAddress := read64(bf)
	logger.Infof("root block address=0x%x", rootBlockAddress)
	rowsRootIndirect := read16(bf)
	logger.Infof("rows in root indirect block=%d", rowsRootIndirect)
	link.rowsRootIndirect = rowsRootIndirect
	h5.checkChecksum(link.heapAddress, 142)
	if rowsRootIndirect > 0 {
		logger.Info("Reading indirect heap block")
		h5.readRootBlock(link, rootBlockAddress, flags, rowsRootIndirect)
	} else {
		logger.Info("Adding direct heap block")
		assert(link.block == nil, "don't overwrite direct heap block")
		link.block = make([]uint64, 1)
		link.block[0] = rootBlockAddress
		h5.readHeapDirectBlock(link, rootBlockAddress, flags, startingBlockSize)
	}
}

func (h5 *HDF5) readLocalHeap(addr uint64, offset uint64) string {
	bf := h5.newSeek(addr, 32)
	checkMagic(bf, 4, "HEAP")
	version := read8(bf)
	checkVal(0, version, "version 0 expected for local heap")
	checkZeroes(bf, 3)
	dsSize := read64(bf)
	flOffset := read64(bf)
	dsAddr := read64(bf)
	logger.Infof("dsSize=%d flOffset=0x%x dsAddr=0x%x", dsSize, flOffset, dsAddr)
	bff := h5.newSeek(dsAddr+offset, int64(dsSize)-int64(offset))
	return readNullTerminatedName(bff, 0)
}

func (h5 *HDF5) readSymbolTableLeaf(parent *object, addr uint64, size uint64, heapAddr uint64) {
	bf := h5.newSeek(addr, 8)
	checkMagic(bf, 4, "SNOD")
	version := read8(bf)
	checkVal(1, version, "version 1 expected for symbol table leaf")
	reserved := read8(bf)
	checkVal(0, reserved, "reserved must be zero")
	numSymbols := read16(bf)

	thisSize := int64(numSymbols) * 40
	logger.Info("number of symbols", numSymbols, "size=", size, "thisSize=", thisSize)
	bf = h5.newSeek(addr+uint64(bf.Count()), thisSize)
	for i := 0; i < int(numSymbols); i++ {
		logger.Info("Start: count=", bf.Count(), "rem=", bf.Rem())
		assert(bf.Rem() >= 24,
			fmt.Sprintln(i, "not enough space to read another entry", bf.Rem()))
		linkNameOffset := read64(bf) // 8
		logger.Infof("%d: link name offset=0x%x", i, linkNameOffset)
		linkName := h5.readLocalHeap(heapAddr, linkNameOffset)
		logger.Infof("%d: link name=%s", i, linkName)
		assert(len(linkName) > 0, "namelen cannot be zero")
		objectHeaderAddress := read64(bf) // 16
		logger.Infof("local symbol table entry=%d header addr=0x%x",
			linkNameOffset, objectHeaderAddress)
		cacheType := read32(bf) // 20
		logger.Info("cacheType", cacheType)
		reserved2 := read32(bf) // 24
		checkVal(0, reserved2, "reserved sb")
		assert(cacheType <= 2, "invalid cache type")
		switch cacheType {
		case 0:
			rem := int(16)
			assert(bf.Rem() >= 16, "not enough data to read symbol table entry")
			logger.Info("no data is cached")
			checkZeroes(bf, rem)
		case 1:
			btreeAddr := read64(bf)
			nameHeapAddr := read64(bf)
			logger.Infof("btree addr=0x%x name heap addr=0x%x", btreeAddr, nameHeapAddr)
		case 2:
			offset := read32(bf)
			checkZeroes(bf, 12)
			logger.Info("Symbolic link offset", offset)
			thrower.Throw(ErrLinkType)
		}
		_, has := parent.children[linkName]
		assert(!has, "duplicate object")
		if h5.hasAddr(objectHeaderAddress) {
			logger.Info("avoid link loop")
			logger.Infof("done with name=%s", string(linkName))
			return
		}
		obj := newObject()
		obj.addr = objectHeaderAddress
		h5.addrs[objectHeaderAddress] = true

		obj.name = linkName
		logger.Infof("object (0x%x, %s) from symbol table, parent (0x%x, %s)\n",
			obj.addr, obj.name, parent.addr, parent.name)
		parent.children[obj.name] = obj
		obj.isGroup = true
		h5.readDataObjectHeader(obj, objectHeaderAddress)
		logger.Info("STE rem=", bf.Rem())
		h5.dumpObject(obj)
		logger.Infof("done with name=%s", obj.name)
	}
}

func (h5 *HDF5) readSymbolTable(parent *object, addr uint64, heapAddr uint64) {
	bf := h5.newSeek(addr, 52) // adjust later

	checkMagic(bf, 4, "TREE") // "SNOD"
	nodeType := read8(bf)
	nodeLevel := read8(bf)
	entriesUsed := read16(bf)
	leftAddress := read64(bf)
	rightAddress := read64(bf)
	logger.Infof("type=%v level=%v entries=%v left=0x%x right=0x%x",
		nodeType, nodeLevel, entriesUsed, leftAddress, rightAddress)
	if nodeLevel > 0 {
		logger.Infof("Start level %d", nodeLevel)
	}
	assert(leftAddress == invalidAddress && rightAddress == invalidAddress,
		"Siblings unexpected")
	assert(nodeType == 0, "what we expect")
	type keyAddr struct {
		key  uint64
		addr uint64
	}
	keyAddrs := []keyAddr{}
	bf = h5.newSeek(addr+uint64(bf.Count()), 16*int64(entriesUsed)+8)
	for i := uint16(0); i < entriesUsed; i++ {
		key := read64(bf)
		childAddr := read64(bf)
		logger.Info("key, childaddr", key, childAddr)
		keyAddrs = append(keyAddrs, keyAddr{key, childAddr})
	}
	lastKey := read64(bf)
	sort.SliceStable(keyAddrs, func(i, j int) bool {
		return keyAddrs[i].key < keyAddrs[j].key
	})
	var prevKey uint64
	prevAddr := invalidAddress
	for i, v := range keyAddrs {
		if prevAddr != invalidAddress {
			logger.Infof("%d: key=%d prevKey=%d size=%d addr=0x%x", i,
				v.key, prevKey, v.key-prevKey, prevAddr)
		}
		prevKey = v.key
		prevAddr = v.addr
	}
	if prevAddr != invalidAddress {
		logger.Infof("last: key=%d prevKey=%d size=%d addr=0x%x",
			lastKey, prevKey, lastKey-prevKey, prevAddr)
	}
	prevAddr = invalidAddress
	for _, v := range keyAddrs {
		if prevAddr != invalidAddress {
			if lastKey > prevKey {
				h5.readSymbolTableLeaf(parent, prevAddr, (v.key - prevKey), heapAddr)
			}
		}
		prevKey = v.key
		prevAddr = v.addr
	}
	if prevAddr != invalidAddress {
		if lastKey > prevKey {
			h5.readSymbolTableLeaf(parent, prevAddr, (lastKey - prevKey), heapAddr)
		}
	}
}

func (h5 *HDF5) readBTree(parent *object, addr uint64) {
	bf := h5.newSeek(addr, 36)
	checkMagic(bf, 4, "BTHD")
	version := read8(bf)
	logger.Info("btree version=", version)
	checkVal(0, version, "bthd version must be zero")
	ty := read8(bf)
	logger.Info("btree type=", ty)
	nodeSize := read32(bf)
	logger.Info("nodesize=", nodeSize)
	recordSize := read16(bf)
	logger.Info("recordsize=", recordSize)
	depth := read16(bf)
	logger.Info("depth=", depth)
	splitPercent := read8(bf)
	logger.Info("splitPercent=", splitPercent)
	mergePercent := read8(bf)
	logger.Info("mergePercent=", mergePercent)
	rootNodeAddress := read64(bf)
	logger.Infof("rootNodeAddress=0x%x", rootNodeAddress)
	numRecRootNode := read16(bf)
	logger.Info("numRecRootNode=", numRecRootNode)
	numRec := read64(bf)
	logger.Info("numRec=", numRec)

	h5.checkChecksum(addr, 34)
	// TODO: indirect blocks for leaf
	if depth > 0 {
		h5.readBTreeInternal(parent, rootNodeAddress, uint64(numRecRootNode), recordSize, depth, nodeSize)
	} else {
		h5.readBTreeLeaf(parent, rootNodeAddress, uint64(numRec), recordSize)
	}
}

func (h5 *HDF5) readLinkInfo(bf io.Reader) *linkInfo {
	version := read8(bf)
	logger.Info("link info version=", version)
	flags := read8(bf)
	logger.Infof("flags=%s", binaryToString(uint64(flags)))
	ci := invalidAddress
	if hasFlag8(flags, 0) {
		ci = read64(bf)
		logger.Infof("ci=%x", ci)
	}
	fha := read64(bf)
	logger.Infof("fda=0x%x", fha)
	bta := read64(bf)
	logger.Infof("bta=0x%x", bta)
	coi := invalidAddress
	if hasFlag8(flags, 1) {
		coi = read64(bf)
		logger.Infof("coi=0x%x", coi)
	}
	return &linkInfo{
		creationIndex:      ci,
		heapAddress:        fha,
		btreeAddress:       bta,
		creationOrderIndex: coi,
		block:              nil,
		iBlock:             nil,
		heapIDLength:       0,
		maxHeapSize:        0,
		blockSize:          0,
	}
}

func hexPrint(addr uint64) string {
	return fmt.Sprintf("0x%x", addr)
}

func (h5 *HDF5) isMagic(magic string, addr uint64) bool {
	assert(addr != 0 && addr != invalidAddress,
		fmt.Sprint("invalid address for checking magic number: ", hexPrint(addr)))
	if addr+4 > uint64(h5.fileSize) {
		logger.Error("seeking past end of file -- probably a truncated file")
		thrower.Throw(ErrCorrupted)
		panic("not reached")
	}
	var b [4]byte
	_, err := h5.file.ReadAt(b[:], int64(addr))
	thrower.ThrowIfError(err)
	bs := string(b[:])
	return bs == magic
}

// This is the same as LinkInfo?
func (h5 *HDF5) readAttributeInfo(bf io.Reader) *linkInfo {
	version := read8(bf)
	logger.Info("attribute version=", version)
	checkVal(0, version, "attribute version")
	flags := read8(bf)
	logger.Infof("flags=%s", binaryToString(uint64(flags)))
	ci := invalidAddress
	if hasFlag8(flags, 0) {
		ci := read16(bf)
		logger.Infof("ci=0x%x", ci)
	}
	fha := read64(bf)
	logger.Infof("fda=0x%x", fha)
	bta := read64(bf)
	logger.Infof("bta=0x%x", bta)
	co := invalidAddress
	if hasFlag8(flags, 1) {
		co = read64(bf)
		logger.Infof("co=0x%x", co)
	}
	return &linkInfo{
		creationIndex:      ci,
		heapAddress:        fha,
		btreeAddress:       bta,
		creationOrderIndex: co,
		block:              nil,
		iBlock:             nil,
		heapIDLength:       0,
		maxHeapSize:        0,
		blockSize:          0,
	}
}

func (h5 *HDF5) readGroupInfo(obf io.Reader) {
	bf := obf.(remReader)
	origSize := bf.Rem()

	version := read8(bf)
	logger.Info("group info version=", version)
	checkVal(0, version, "group info version")
	flags := read8(bf)
	logger.Infof("flags=%s", binaryToString(uint64(flags)))
	if hasFlag8(flags, 0) {
		// this flag is never set
		assert(bf.Rem() >= 4, "mcv/mdv size")
		mcv := read16(bf)
		logger.Infof("mcv=0x%x", mcv)
		mdv := read16(bf)
		logger.Infof("mdv=0x%x", mdv)
		fail("group info link phase change flags not supported")
	}
	if hasFlag8(flags, 1) {
		// this flag is never set
		assert(bf.Rem() >= 4, "ene/elnl size")
		ene := read16(bf)
		logger.Infof("elnl=0x%x", ene)
		elnl := read16(bf)
		logger.Infof("elnl=0x%x", elnl)
		fail("group info esimated numbers not supported")
	}
	if bf.Rem() > 0 {
		// Due to a bug with ncgen, extra bytes can appear here.
		// Allow them
		n := bf.Rem()
		checkZeroes(bf, int(n))
		logger.Info("ignore", n, "remaining bytes in Group Info message.",
			"origsize=", origSize)
	}
}

func headerTypeToString(ty int) string {
	if ty < 0 || ty >= len(htts) {
		return fmt.Sprintf("unknown header type 0x%x", ty)
	}
	return htts[ty]
}

func (h5 *HDF5) readDataspace(obf io.Reader) ([]uint64, int64) {
	bf := obf.(remReader)
	version := read8(bf)
	logger.Info("dataspace message version=", version)
	assertError(version == 1 || version == 2,
		ErrDataspaceVersion,
		fmt.Sprint("dataspace version not supported: ", version))
	d := read8(bf)
	logger.Info("dataspace dimensionality=", d)
	flags := read8(bf)
	logger.Info("dataspace flags=", binaryToString(uint64(flags)))
	dstype := read8(bf)
	if version == 1 {
		checkVal(0, dstype, fmt.Sprint("Reserved not zero: ", dstype))
		dstype = 1
		reserved := read32(bf)
		checkVal(0, reserved, "reserved")
	}
	logger.Info("dataspace type=", dstype)
	switch dstype {
	case 0:
		logger.Infof("scalar dataspace")
	case 1:
		logger.Infof("simple dataspace")
	case 2:
		logger.Infof("null dataspace")
		// let it go
	default:
		fail(fmt.Sprintf("unknown dstype %d", dstype))
	}
	ret := make([]uint64, d)
	count := int64(1)
	for i := 0; i < int(d); i++ {
		sz := read64(bf)
		logger.Infof("dataspace dimension %d/%d size=%d", i, d, sz)
		ret[i] = sz
		count *= int64(sz)
	}
	if hasFlag8(flags, 0) {
		for i := 0; i < int(d); i++ {
			sz := read64(bf)
			if sz == unlimitedSize {
				logger.Infof("dataspace maximum dimension %d/%d UNLIMITED", i, d)
			} else {
				logger.Infof("dataspace maximum dimension %d/%d size=%d", i, d, sz)
			}
		}
	}
	if version == 1 && hasFlag8(flags, 1) {
		// has not been seen in the wild
		for i := 0; i < int(d); i++ {
			pi := read64(bf)
			logger.Infof("dataspace permutation index %d/%d = %d", i, d, pi)
		}
		fail("permutation indices not supported")
	}
	if version == 2 && dstype == 2 && bf.Rem() > 0 {
		logger.Info("Null v2 flags=", flags, "d=", d, "rem=", bf.Rem())
	}
	return ret, count
}

func (h5 *HDF5) readFilterPipeline(obj *object, obf io.Reader) {
	bf := obf.(remReader)
	logger.Infof("pipeline size=%d", bf.Rem())
	version := read8(bf)
	logger.Infof("pipeline version=%d", version)
	assert(version >= 1 && version <= 2, "pipeline versin")
	nof := read8(bf)
	logger.Infof("pipeline filters=%d", nof)
	if version == 1 {
		reserved := read16(bf)
		checkVal(0, reserved, "reserved")
		reserved2 := read32(bf)
		checkVal(0, reserved2, "reserved")
	}
	for i := 0; i < int(nof); i++ {
		fiv := read16(bf)
		nameLength := uint16(0)
		if version == 1 || fiv >= 256 {
			nameLength = read16(bf)
		}
		flags := read16(bf)
		nCDV := read16(bf)
		logger.Infof("fiv=%d name length=%d flags=%s ncdv=%d",
			fiv, nameLength, binaryToString(uint64(flags)), nCDV)
		if nameLength > 0 {
			b := make([]byte, nameLength)
			read(bf, b)
			logger.Infof("filter name=%s", getString(b))
			padBytes(bf, 7)
		}
		cdv := make([]uint32, nCDV)
		for i := 0; i < int(nCDV); i++ {
			assert(bf.Rem() >= 4, fmt.Sprintf("short read on client data (%d)", bf.Rem()))
			cd := read32(bf)
			cdv[i] = cd
			logger.Infof("client data[%d] = 0x%x", i, cd)
		}
		if version == 1 && nCDV%2 == 1 {
			pad := read32(bf)
			checkVal(0, pad, "pad is not zero")
		}
		switch fiv {
		case filterDeflate, filterShuffle, filterFletcher32:

		default:
			thrower.Throw(ErrUnsupportedFilter)
		}
		obj.filters = append(obj.filters, filter{fiv, cdv})
	}
}

func (h5 *HDF5) readDataLayout(parent *object, obf io.Reader) {
	bf := obf.(remReader)
	logger.Infof("layout size=%d", bf.Rem())
	version := read8(bf)
	// V4 is quite complex and not supported yet, but we parse some of it
	switch version {
	case 3, 4:
		break
	default:
		// Read away rest of record that we don't understand
		skip(bf, bf.Rem())
		failError(ErrLayout, fmt.Sprint("unsupported layout version: ", version))
	}
	class := read8(bf)
	logger.Infof("layout version=%d class=%d", version, class)
	switch class {
	case classCompact:
		size := read16(bf)
		logger.Infof("layout compact size=%d", size)
		b := make([]byte, size)
		read(bf, b)
		parent.dataBlocks = append(parent.dataBlocks,
			dataBlock{
				offset:     0,
				length:     uint64(len(b)),
				dsOffset:   0,
				dsLength:   uint64(len(b)),
				filterMask: 0,
				offsets:    nil,
				rawData:    b,
			})
	case classContiguous:
		address := read64(bf)
		size := read64(bf)
		logger.Infof("layout contiguous address=0x%x size=%d", address, size)
		if address != invalidAddress {
			logger.Infof("alloc blocks")
			parent.dataBlocks = append(parent.dataBlocks,
				dataBlock{address, uint64(size), 0, uint64(size), 0, nil, nil})
		}
	case classChunked:
		var flags uint8 // v4 only
		if version == 4 {
			flags = read8(bf)
		}
		dimensionality := read8(bf)
		switch version {
		case 3:
			address := read64(bf)
			logger.Infof("layout dimensionality=%d address=0x%x", dimensionality, address)
			numberOfElements := uint64(1)
			assertError(dimensionality >= 2,
				ErrDimensionality,
				fmt.Sprint("Invalid dimensionality ", dimensionality))

			layout := make([]uint64, int(dimensionality)-1)
			for i := 0; i < int(dimensionality)-1; i++ {
				size := read32(bf)
				numberOfElements *= uint64(size)
				layout[i] = uint64(size)
				logger.Info("layout", i, "size", size)
			}
			parent.objAttr.layout = layout

			size := read32(bf)
			logger.Infof("layout data element size=%d, number of elements=%d", size,
				numberOfElements)
			if address != invalidAddress {
				h5.readBTreeNode(parent, address, uint64(size), numberOfElements, dimensionality)
			} else {
				logger.Info("layout specified invalid address")
			}

		case 4:
			logger.Infof("V4 flags=%x", flags)
			if hasFlag8(flags, 0) {
				logger.Info("do not apply filter to partial edge trunk flag")
			}
			if hasFlag8(flags, 1) {
				logger.Info("filtered chunk for single chunk indexing")
			}
			logger.Info("v4 dimensionality", dimensionality)
			encodedLen := read8(bf)
			logger.Info("encoded length", encodedLen)
			assert(encodedLen > 0 && encodedLen <= 8, "invalid encoded length")
			layout := make([]uint64, int(dimensionality))
			numberOfElements := uint64(1)

			for i := 0; i < int(dimensionality); i++ {
				size := readEnc(bf, encodedLen)
				numberOfElements *= uint64(size)
				layout[i] = size
				logger.Info("layout", i, "size", size)
			}
			parent.objAttr.layout = layout
			cit := read8(bf)
			logger.Info("chunk indexing type", cit)
			assertError(cit >= 1 && cit <= 5, ErrLayout,
				"bad value for chunk indexing type")
			switch cit {
			case 1:
				// Single-chunk indexing
				fchunksize := read64(bf)
				logger.Info("chunk size = ", fchunksize)
				if hasFlag8(flags, 0) {
					filters := read32(bf)
					logger.Info("filters = ", filters)
				}
				failError(ErrVersion, "single chunk indexing not supported")
			case 2:
				logger.Info("implicit indexing")
			case 3:
				pageBits := read8(bf)
				logger.Info("fixed array pagebits=", pageBits)
			case 4:
				// Extensible-array indexing is a superblock v3 feature
				maxbits := read8(bf)
				indexElements := read8(bf)
				minPointers := read8(bf)
				minElements := read8(bf)
				pageBits := read8(bf) // doc says 16-bit, but is wrong
				logger.Info("extensible array mb=", maxbits,
					"ie=", indexElements, "mp=", minPointers, "me=", minElements,
					"pb=", pageBits)
				failError(ErrVersion, "extensible array indexing not supported")
			case 5:
				// btree array indexing is a superblock v3 feature
				nodeSize := read32(bf)
				splitPercent := read8(bf)
				mergePercent := read8(bf)
				logger.Info("b-tree indexing size=", nodeSize, "split%=", splitPercent, "merge%=", mergePercent)
				failError(ErrVersion, "Version 2 B-tree array indexing not supported")
			}
			rem := bf.Rem()
			var address uint64
			switch rem {
			case 8:
				address = read64(bf)
				logger.Infof("v4 address=0x%x", address)
				rem -= 8
			default:
				logger.Infof("Expected an 8-byte address, got a %d-byte one", rem)
				b := make([]byte, rem)
				read(bf, b)
				fail(fmt.Sprint("Remaining bytes len=", rem, " val=", b))
			}
			thrower.Throw(ErrLayout)
		}
	case classVirtual:
		logger.Error("Virtual storage not supported")
		thrower.Throw(ErrVirtualStorage)
	default:
		fail("bad class")
	}
}

func (h5 *HDF5) readFillValue(bf io.Reader) []byte {
	version := read8(bf)
	assert(version >= 1 && version <= 3, "fill value version")
	logger.Info("fill value version", version)
	var spaceAllocationTime byte
	var fillValueWriteTime byte
	var fillValueDefined byte
	var fillValueUnDefined byte
	switch version {
	case 1, 2:
		spaceAllocationTime = read8(bf)
		fillValueWriteTime = read8(bf)
		fillValueDefined = read8(bf)
	case 3:
		flags := read8(bf)
		spaceAllocationTime = flags & 0b11
		fillValueWriteTime = (flags >> 2) & 0b11
		fillValueUnDefined = (flags >> 4) & 0b1
		fillValueDefined = (flags >> 5) & 0b1
		reserved := (flags >> 6) & 0b11
		checkVal(0, reserved, "extra bits in fill value")
		if fillValueUnDefined == 0b1 {
			// fillValueUndefined never seems to be set
			logger.Warn("executing fill value undefined code for first time")
			if fillValueDefined == 0b1 {
				fail("Cannot have both defined and undefined fill value")
			}
			logger.Warnf("undefined fill value")
			return fillValueUndefinedConstant // only the pointer is used
		}
	}
	switch spaceAllocationTime {
	case 1, 2, 3:
		logger.Infof("space allocation time=%d", spaceAllocationTime)
	default:
		fail(fmt.Sprintf("invalid space allocation time=0x%x", spaceAllocationTime))
	}

	switch fillValueWriteTime {
	case 0, 1, 2:
		logger.Infof("fill value write time=%d", fillValueWriteTime)
	default:
		fail(fmt.Sprintf("invalid fill value write time=0x%x", fillValueWriteTime))
	}

	logger.Info("fill value defined=", fillValueDefined)

	if version > 1 && fillValueDefined == 0 {
		logger.Infof("default fill value")
		return nil // default is zero
	}
	// Read the fill value
	len := read32(bf)
	if len == 0 {
		logger.Infof("zero length fill value")
		return nil // zero-length, maybe they meant zero
	}
	b := make([]byte, len)
	read(bf, b)
	logger.Infof("fill value=0x%x len=%d", b, len)
	return b
}

func (h5 *HDF5) readDatatype(obj *object, bf io.Reader) *attribute {
	size := bf.(remReader).Rem()
	logger.Infof("going to read %v bytes", size)
	logger.Info("print datatype with properties from chunk")
	var objAttr attribute
	pf := newResetReader(bf, bf.(remReader).Rem())
	printDatatype(h5, h5, pf, nil, 0, &objAttr)
	return &objAttr
}

func (h5 *HDF5) readCommon(obj *object, obf io.Reader, version uint8, ohFlags byte, origAddr uint64, chunkSize uint64) {
	logger.Infof("readCommon origAddr=0x%x", origAddr)
	bf := newResetReader(obf, int64(chunkSize))
	logger.Info("top chunksize", chunkSize, "nRead", bf.Count(), "rem", bf.Rem())
	for bf.Rem() >= 3 {
		var headerType uint16
		if version == 1 {
			headerType = read16(bf)
		} else {
			headerType = uint16(read8(bf))
		}
		logger.Infof("header message type=%s (%d) version=%d",
			headerTypeToString(int(headerType)), headerType, version)

		size := read16(bf)
		logger.Info("size of header message data=", size)
		if size == 0 && version == 1 {
			logger.Info("--- zero sized ---")
			logger.Info("zs chunksize", chunkSize, "nRead", bf.Count(), "rem", bf.Rem())
			continue
		}
		if bf.Count() == int64(chunkSize) {
			logger.Info("no chunks left for flags")
			break
		}
		nReadSave := bf.Count() // version 1 calculates things differently
		hFlags := read8(bf)
		if version == 1 {
			checkZeroes(bf, 3)
		}
		if hasFlag8(hFlags, 0) {
			logger.Info("header message flag: constant message")
		} else {
			logger.Info("header message flag: NOT constant message")
		}
		if hasFlag8(hFlags, 2) {
			logger.Info("header message flag: do not share message")
		}
		if hasFlag8(hFlags, 3) {
			logger.Info("header message flag: do not open if writing file")
		}
		if hasFlag8(hFlags, 4) {
			logger.Info("header message flag: set bit 5 if you don't understand this object")
		}
		if hasFlag8(hFlags, 5) {
			logger.Info("header message flag: has object someone didn't understand")
		}
		if hasFlag8(hFlags, 6) {
			logger.Info("header message flag: message is sharable")
		}
		if hasFlag8(hFlags, 7) {
			logger.Info("header message flag: must fail to open if you don't understand type")
		}

		if hasFlag8(ohFlags, 2) {
			if bf.Rem() < 2 {
				logger.Info("no chunks to read creation order")
				break
			}
			co := read16(bf)
			logger.Infof("creation order = %d", co)
		}
		if size == 0 && version > 1 {
			logger.Info("--- zero sized ---")
			logger.Info("zs2 chunksize", chunkSize, "nRead", bf.Count(), "rem", bf.Rem())
			continue
		}
		if version == 1 {
			logger.Infof("rem=%v size=%v", bf.Rem(), size)
			used := bf.Count() - nReadSave
			logger.Infof("used %d bytes", used)
			assert(int64(size) <= bf.Rem(),
				fmt.Sprintf("not enough space %v %v", size, bf.Rem()))
		}
		if version > 1 {
			nReadSave = bf.Count()
		}
		assert(uint64(size) <= (chunkSize-uint64(nReadSave)),
			fmt.Sprint("too big: ", size, chunkSize, nReadSave))
		if hasFlag8(hFlags, 1) {
			// var d = make([]byte, size)
			// read(bf, d)
			f := newResetReader(bf, int64(size))
			length := read16(f)
			logger.Info("shared message length", length)
			addr := read64(f)
			logger.Infof("shared message addr = 0x%x", addr)
			_ = h5.getSharedAttr(obj, addr)
			checkZeroes(f, int(f.Rem()))

			// TODO: we need to store addr and dtb somewhere, it will get used later
			logger.Info("shared attr dtversion", obj.objAttr.dtversion)
			// TODO: what else might we need to copy? dimensions?
			continue
		}
		if version == 1 {
			logger.Info("About to read v=", version)
		}
		f := newResetReader(bf, int64(size))
		switch headerType {
		case typeNIL:
			skip(f, int64(size))
			logger.Infof("nil -- do nothing (%d bytes)", size)

		case typeDataspace:
			obj.isGroup = false
			obj.objAttr.dimensions, _ = h5.readDataspace(f)
			logger.Info("dimensions are", obj.objAttr.dimensions)

		case typeLinkInfo:
			logger.Info("Link Info")
			assert(obj.link == nil, "already have a link")
			obj.link = h5.readLinkInfo(f)
			obj.isGroup = true

		case typeDatatype:
			obj.isGroup = false
			logger.Info("Datatype")
			// hacky: fix

			save := obj.objAttr.dimensions
			noDf := obj.objAttr.noDf
			obj.objAttr = h5.readDatatype(obj, f)
			h5.sharedAttrs[obj.addr] = obj.objAttr
			logger.Info("dimensions are", obj.objAttr.dimensions)
			obj.objAttr.dimensions = save
			obj.objAttr.noDf = noDf

		case typeDataStorageFillValueOld:
			obj.isGroup = false
			logger.Info("Fill value old")
			sz := read32(f)
			logger.Info("Fill value old size", sz)
			fv := make([]byte, sz)
			read(f, fv)
			obj.fillValueOld = fv
			logger.Infof("Fill value old=0x%x", fv)

		case typeDataStorageFillValue:
			// this may not be used in netcdf
			obj.isGroup = false
			fv := h5.readFillValue(f)
			if fv == nil {
				logger.Info("undefined or default fill value")
				break
			}
			obj.fillValue = fv
			logger.Infof("Fill value=0x%x", fv)

		case typeLink:
			logger.Info("XXX: Link")
			h5.readLinkDirectFrom(obj, f, size, 0)

		case typeExternalDataFiles:
			logger.Error("We don't handle external data files")
			thrower.Throw(ErrExternal)

		case typeDataLayout:
			obj.isGroup = false
			h5.readDataLayout(obj, f)

		case typeBogus:
			// for testing only
			bogus := read32(f)
			assert(bogus == 0xdeadbeef, "bogus")

		case typeGroupInfo:
			h5.readGroupInfo(f)

		case typeDataStorageFilterPipeline:
			h5.readFilterPipeline(obj, f)

		case typeAttribute:
			logger.Infof("Attribute, obj addr=0x%x", obj.addr)
			h5.readAttribute(obj, f, 0)

		case typeObjectComment:
			comment := readNullTerminatedName(f, 0)
			logger.Info("Comment=", comment)

		case typeObjectModificationTimeOld:
			get := func(size int) string {
				b := make([]byte, size)
				read(f, b)
				return string(b)
			}
			year := get(4)    // 4
			month := get(2)   // 6
			day := get(2)     // 8
			hour := get(2)    // 10
			minute := get(2)  // 12
			second := get(2)  // 14
			checkZeroes(f, 2) // 16
			logger.Infof("Old mod time %s-%s-%s %s:%s:%s", year, month, day, hour, minute, second)

		case typeSharedMessageTable:
			assertError(false, ErrSuperblock, "shared message table not handled")

		case typeObjectHeaderContinuation:
			h5.readContinuation(obj, f, version, ohFlags)

		case typeSymbolTableMessage:
			btreeAddr := read64(f)
			heapAddr := uint64(math.MaxUint64)
			heapAddr = read64(f)
			logger.Infof("Symbol table btree=0x%x heap=0x%x", btreeAddr, heapAddr)

			h5.readSymbolTable(obj, btreeAddr, heapAddr)

		case typeObjectModificationTime:
			// this may not be used in netcdf
			logger.Info("Object Modification Time")
			v := read8(f)
			logger.Info("object modification time version=", v)
			for i := 0; i < 3; i++ {
				z := read8(f)
				checkVal(0, z, "zero")
			}
			time := read32(f)
			logger.Info("seconds since 1970:", time)

		case typeAttributeInfo:
			assert(obj.attr == nil, "already have attr info")
			obj.attr = h5.readAttributeInfo(f)

		case typeBtreeKValues:
			assertError(false, ErrSuperblock, "we don't handle btree k values")

		case typeDriverInfo:
			fail("we don't handle driver info")

		case typeObjectReferenceCount:
			v := read8(f)
			checkVal(0, v, "version")
			refCount := read32(f)
			logger.Info("Reference count:", refCount)

		default:
			b := make([]byte, f.Rem())
			read(f, b)
			maybeFail(fmt.Sprintf("Unknown header type 0x%x data=%x", headerType, b))
		}
		logger.Info("mid chunksize", chunkSize, "nRead", bf.Count(), "rem",
			bf.Rem())
		rem := f.Rem()
		if rem > 0 {
			switch version {
			case 1:
				// V1 is allowed to have padding bytes
				if rem < 8 {
					// allowed for padding up to 8-byte boundary
				} else {
					// This happens with compound data in older files.  It appears there
					// was a bug in the old code that would write out the type information multiple
					// times.
					logger.Infof("V1: %d junk bytes at end of record type=%s", rem,
						headerTypeToString(int(headerType)))
				}
				checkZeroes(f, int(rem))
			case 2:
				// No padding allowed in V2, still we get these.
				logger.Infof("V2: %d junk bytes at end of record type=%s", rem,
					headerTypeToString(int(headerType)))
				checkZeroes(f, int(rem))
			}
		}
	}
	logger.Info("end chunksize", chunkSize, "nRead", bf.Count(), "rem", bf.Rem())
	rem := bf.Rem()
	if rem > 0 {
		logger.Info("junk bytes at end: ", rem)
		checkZeroes(bf, int(rem))
	}
}

func (h5 *HDF5) readContinuation(obj *object, obf io.Reader, version uint8, ohFlags byte) {
	offset := read64(obf)
	size := read64(obf)
	logger.Infof("continuation offset=%08x length=%d", offset, size)
	bf := h5.newSeek(offset, int64(size))
	chunkSize := size
	start := 0
	if version > 1 {
		checkMagic(bf, 4, "OCHK")
		chunkSize = size - 8 // minus magic & checksum
		start = 4            // skip magic
	}
	logger.Info("read data object header - continuation")
	h5.readCommon(obj, bf, version, ohFlags, offset+uint64(start), chunkSize)
	logger.Info("done reading continuation")
	if version > 1 {
		if bf.Count() < (int64(size - 4)) {
			// Gaps have not been seen in the wild.
			gap := (int64(size) - 4) - bf.Count()
			logger.Info(bf.Count(), "bytes read", "gap end=", (size - 4),
				"gap size=", gap, "bytes rem=", bf.Rem())
			if gap > bf.Rem() {
				logger.Warn("gap bigger than rem=", bf.Rem())
				gap = bf.Rem()
			}
			checkZeroes(bf, int(gap))
		}
		h5.checkChecksum(offset, int(size)-4)
	}
}

func (h5 *HDF5) readDataObjectHeader(obj *object, addr uint64) {
	// Hacky: there must be a better way to determine V1 object headers
	if h5.isMagic("OHDR", addr) {
		h5.readDataObjectHeaderV2(obj, addr)
		return
	}
	h5.readDataObjectHeaderV1(obj, addr)
}

func (h5 *HDF5) readDataObjectHeaderV2(obj *object, addr uint64) {
	obj.addr = addr
	origAddr := addr
	logger.Infof("read object header %x", addr)
	bf := h5.newSeek(addr, 6) // minimum size, not including header message data or checksum
	checkMagic(bf, 4, "OHDR")
	version := read8(bf)
	logger.Info("object header version=", version)
	checkVal(2, version, "only handle version 2")

	ohFlags := read8(bf)
	logger.Infof("flags=%s", binaryToString(uint64(ohFlags)))

	timePresent := false
	maxPresent := false
	if hasFlag8(ohFlags, 2) {
		logger.Info("attribute creation order tracked")
	}
	if hasFlag8(ohFlags, 3) {
		logger.Info("attribute creation order indexed")
	}
	if hasFlag8(ohFlags, 4) {
		logger.Info("attribute storage phase change values stored")
		maxPresent = true
	}
	if hasFlag8(ohFlags, 5) {
		logger.Info("access, mod, change and birth times are stored")
		timePresent = true
	}
	assert(ohFlags&0xc0 == 0, "reserved fields should not be present")

	if timePresent {
		// we need 16 more bytes for 4 4-byte fields
		addr += uint64(bf.Count())
		assert(bf.Rem() == 0, "should use all bytes")
		bf = h5.newSeek(addr, 16+bf.Rem())
		i := read32(bf)
		t := time.Unix(int64(i), 0)
		logger.Infof("access time=%s", t.UTC().Format(time.RFC3339))
		i = read32(bf)
		t = time.Unix(int64(i), 0)
		logger.Infof("mod time=%s", t.UTC().Format(time.RFC3339))
		i = read32(bf)
		t = time.Unix(int64(i), 0)
		logger.Infof("change time=%s", t.UTC().Format(time.RFC3339))
		i = read32(bf)
		t = time.Unix(int64(i), 0)
		logger.Infof("birth time=%s", t.UTC().Format(time.RFC3339))
		// TODO: store these times and provide an API to view them
	}
	if maxPresent {
		// maxPresent never seems to be true
		// we need 4 more bytes for 2 2-byte fields
		addr += uint64(bf.Count())
		assert(bf.Rem() == 0, "should use all bytes")
		bf = h5.newSeek(addr, 4+bf.Rem())
		// These don't matter for read-only.
		s := read16(bf)
		logger.Info("max compact=", s)
		s = read16(bf)
		logger.Info("max dense=", s)
	}

	// Bits 0-1 of the flags determine the size of the first chunk
	nBytesInChunkSize := 1 << (ohFlags & 0b11)

	// we need nBytesInChunksize more bytes to read chunkSize
	addr += uint64(bf.Count())
	assert(bf.Rem() == 0, "should use all bytes")
	bf = h5.newSeek(addr, int64(nBytesInChunkSize)+bf.Rem())
	chunkSize := readEnc(bf, uint8(nBytesInChunkSize))
	// we need chunkSize more bytes to read headers
	addr += uint64(bf.Count())
	assert(bf.Rem() == 0, "should use all bytes")
	bf = h5.newSeek(addr, int64(chunkSize)+bf.Rem())

	// Read fields that object header and continuation blocks have in common
	logger.Info("size of chunk=", chunkSize)
	obj.children = make(map[string]*object)
	start := bf.Count()
	h5.readCommon(obj, bf, version, ohFlags, addr, chunkSize)
	used := bf.Count() - start
	assert(used == int64(chunkSize),
		fmt.Sprintf("readCommon should read %d bytes, read %d, delta %d",
			chunkSize, used, int64(chunkSize)-used))
	addr += chunkSize

	// Finally, compute the checksum
	h5.checkChecksum(origAddr, int(addr-origAddr))
	logger.Infof("obj %s at addr 0x%x\n", obj.name, origAddr)
}

func (h5 *HDF5) readDataObjectHeaderV1(obj *object, addr uint64) {
	obj.addr = addr
	logger.Infof("v1 addr=0x%x", addr)
	bf := h5.newSeek(addr, 16)
	version := read8(bf)
	logger.Info("v1 object header version=", version)
	switch version {
	case 1:
		break
	case 0:
		logger.Warn("Data object header version should be 1, but is zero. Continuing anyway.")
	default:
		fail(fmt.Sprintf("Invalid data object header version: %d", version))
	}

	reserved := read8(bf)
	checkVal(0, reserved, "reserved")

	numMessages := read16(bf)
	referenceCount := read32(bf)
	headerSize := read32(bf)
	logger.Info("Num messages", numMessages, "reference count", referenceCount,
		"header size", headerSize)

	// Read fields that object header and continuation blocks have in common
	obj.children = make(map[string]*object)
	checkZeroes(bf, 4)
	count := uint64(bf.Count())
	bf = h5.newSeek(addr+count, int64(headerSize))
	h5.readCommon(obj, bf, version, 0, addr+count, uint64(headerSize))
	logger.Info("done reading chunks")
}

// Close closes this group and closes any underlying files if they are no
// longer being used by any other groups.
func (h5 *HDF5) Close() {
	if h5.file != nil {
		h5.file.Close()
	}
	h5.file = nil
}

func canonicalizePath(s string) string {
	prefix := ""
	if strings.HasPrefix(s, "/") {
		prefix = "/"
	}
	spl := strings.Split(s, "/")
	nspl := []string{}
	for i := range spl {
		if spl[i] == "" {
			continue
		}
		nspl = append(nspl, spl[i])
	}
	return prefix + strings.Join(nspl, "/")
}

// GetGroup gets the given group or returns an error if not found.
// The group can start with "/" for absolute names, or relative.
func (h5 *HDF5) GetGroup(group string) (g api.Group, err error) {
	defer thrower.RecoverError(&err)
	var groupName string
	group = canonicalizePath(group)
	toDescend := h5.groupObject
	h5groupName := h5.groupName
	switch {
	case strings.HasPrefix(group, "/"):
		// Absolute path
		if group != "/" {
			groupName = group + "/"
		} else {
			groupName = "/"
		}
		toDescend = h5.rootObject
		h5groupName = "/"
	default:
		// Relative path
		groupName = h5.groupName + group + "/"
	}

	var sgDescend func(obj *object, group string) *object
	sgDescend = func(obj *object, group string) *object {
		if !obj.isGroup {
			return nil
		}
		if group == groupName {
			return obj
		}
		desc := groupName[len(group):]
		spl := strings.Split(desc, "/")
		o, has := obj.children[spl[0]]
		if has {
			ret := sgDescend(o, group+o.name+"/")
			if ret != nil {
				return ret
			}
		}
		return nil
	}

	o := sgDescend(toDescend, h5groupName)
	if o == nil {
		return nil, ErrNotFound
	}

	hg := *h5
	hg.groupName = groupName
	hg.groupObject = o
	hg.file = h5.file.dup()
	return api.Group(&hg), nil
}

func fileSize(file io.ReadSeeker) int64 {
	fi, err := file.Seek(0, io.SeekEnd)
	file.Seek(0, io.SeekStart)
	thrower.ThrowIfError(err)
	return fi
}

// Open is the implementation of the API netcdf.Open.
// Using netcdf.Open is preferred over using this directly.
func Open(fname string) (nc api.Group, err error) {
	defer thrower.RecoverError(&err)
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	c, err := New(file)
	if err != nil {
		file.Close()
	}
	return c, err
}

// New is the implementation of the API netcdf.New.
// Using netcdf.New is preferred over using this directly.
func New(file api.ReadSeekerCloser) (nc api.Group, err error) {
	defer thrower.RecoverError(&err)
	fileSize := fileSize(file)
	var fname string
	if f, ok := file.(*os.File); ok {
		fname = f.Name()
		logger.Info("Opened", fname)
	}
	h5 := &HDF5{
		fname:         fname,
		fileSize:      fileSize,
		groupName:     "/",
		file:          newRaFile(file),
		rootAddr:      0,
		root:          nil,
		attribute:     nil,
		rootObject:    nil,
		groupObject:   nil,
		sharedAttrs:   make(map[uint64]*attribute),
		registrations: make(map[string]interface{}),
		addrs:         make(map[uint64]bool),
	}
	h5.readSuperblock()
	assert(h5.rootAddr != invalidAddress, "No root address")
	h5.rootObject = newObject()
	h5.readDataObjectHeader(h5.rootObject, h5.rootAddr)
	h5.groupObject = h5.rootObject
	h5.groupObject.isGroup = true
	h5.dumpObject(h5.rootObject)
	return api.Group(h5), nil
}

func newObject() *object {
	var obj object
	obj.objAttr = &attribute{}
	return &obj
}

func (h5 *HDF5) dumpObject(obj *object) {
	if obj.attr != nil && obj.attr.heapAddress != invalidAddress {
		h5.readHeap(obj.attr)
		h5.readBTree(obj, obj.attr.btreeAddress)
	}
	// then groups
	if obj.link != nil && obj.link.heapAddress != invalidAddress {
		h5.readHeap(obj.link)
		h5.readBTree(obj, obj.link.btreeAddress)
	}
}

func makeSlices(ty reflect.Type, dimLengths []uint64) reflect.Value {
	sliceType := reflect.SliceOf(ty)
	for i := 1; i < len(dimLengths); i++ {
		sliceType = reflect.SliceOf(sliceType)
	}
	return reflect.MakeSlice(sliceType, int(dimLengths[0]), int(dimLengths[0]))
}

// Strings are already slices, so special case them
func makeStringSlices(dimLengths []uint64) reflect.Value {
	sliceType := reflect.TypeOf("")
	for i := 1; i < len(dimLengths); i++ {
		sliceType = reflect.SliceOf(sliceType)
	}
	return reflect.MakeSlice(sliceType, int(dimLengths[0]), int(dimLengths[0]))
}

// check: whether or not to fail if padded bytes are not zeroed.  They
// are supposed to be zero, but software exists out there that does not
// zero them for opaque types.
func padBytesCheck(obf io.Reader, pad32 int, round bool,
	logFunc func(v ...interface{})) bool {
	cbf := obf.(remReader)
	success := true
	var extra int
	if round {
		pad64 := int64(pad32)
		rounded := (cbf.Count() + pad64) & ^pad64
		extra = int(rounded) - int(cbf.Count())
	} else {
		extra = pad32
	}
	if extra > 0 {
		logger.Info(cbf.Count(), "prepad", extra, "bytes")
		b := make([]byte, extra)
		read(cbf, b)
		for i := 0; i < int(extra); i++ {
			if b[i] != 0 {
				success = false
			}
		}
		if !success {
			logFunc(fmt.Sprintf("Reserved not zero len=%d 0x%x", extra, b))
		}
	}
	return success
}

func readAll(bf io.Reader, b []byte) (uint64, error) {
	tot := uint64(0)
	for {
		n, err := bf.Read(b)
		if n == 0 {
			break
		}
		tot += uint64(n)
		b = b[n:]
		if err == io.EOF {
			return tot, err
		}
		if err != nil {
			logger.Error("Some other error", err)
			return tot, err
		}
	}
	return tot, nil
}

func (h5 *HDF5) newRecordReader(obj *object, zlibFound bool, zlibParam uint32,
	shuffleFound bool, shuffleParam uint32, fletcher32Found bool) io.Reader {
	nBlocks := len(obj.dataBlocks)
	size := uint64(calcAttrSize(obj.objAttr))
	if size == 0 {
		return newResetReaderFromBytes([]byte{})
	}
	logger.Info("size=", size, "dtlength=", obj.objAttr.length, "dims=", obj.objAttr.dimensions)
	firstOffset := uint64(0)
	lastOffset := uint64(size)
	segments := newSegments()
	if obj.objAttr.isSlice {
		if len(obj.objAttr.dimensions) > 0 && obj.objAttr.dimensions[0] > 0 {
			dimSize := size / obj.objAttr.dimensions[0]
			firstOffset = uint64(obj.objAttr.firstDim) * dimSize
			lastOffset = uint64(obj.objAttr.lastDim) * dimSize
			size = lastOffset - firstOffset
		}
	}
	if nBlocks == 0 {
		logger.Info("No blocks, filling only", size, obj.objAttr.dimensions)
		return makeFillValueReader(obj, nil, int64(size))
	}
	offset := uint64(0)
	for i, val := range obj.dataBlocks {
		dsLength := val.dsLength
		skipBegin := uint64(0)
		skipEnd := uint64(0)
		valOffset := val.offset

		if offset+dsLength <= firstOffset {
			offset += dsLength
			continue
		}
		if offset >= lastOffset {
			break
		}
		if firstOffset >= offset && firstOffset < offset+dsLength {
			skipBegin = (firstOffset - offset)
		}
		if offset+dsLength > lastOffset {
			skipEnd = offset + dsLength - lastOffset
		}

		assert(val.filterMask == 0,
			fmt.Sprintf("filter mask = 0x%x", val.filterMask))
		logger.Infof("block %d is 0x%x, len %d (%d, %d), mask 0x%x size %d",
			i, val.offset, val.length, val.dsOffset, val.dsLength, val.filterMask, size)
		var bf io.Reader
		canSeek := false
		if val.rawData != nil {
			bf = newResetReaderFromBytes(val.rawData[skipBegin : dsLength+skipBegin])
		} else {
			logger.Infof("offset=0x%x length=%d offset+length=0x%x filesize=0x%x",
				valOffset, val.length,
				valOffset+val.length, h5.fileSize)
			bf = h5.newSeek(valOffset, int64(val.length))
			canSeek = true
		}
		if fletcher32Found {
			logger.Info("Found fletcher32", val.length)
			bf = newFletcher32Reader(bf, val.length)
			if firstOffset > 0 {
				skip(bf, int64(firstOffset))
			}
		}
		if zlibFound {
			logger.Info("trying zlib")
			if zlibParam != 0 {
				logger.Info("zlib param", zlibParam)
			}
			zbf, err := zlib.NewReader(bf)
			if err != nil {
				logger.Error(ErrUnknownCompression)
				return nil
			}
			bf = newResetReader(zbf, int64(dsLength))
			if firstOffset > 0 {
				skip(bf, int64(firstOffset))
			}
		}
		if shuffleFound {
			logger.Info("using shuffle", dsLength)
			bf = newUnshuffleReader(bf, dsLength, shuffleParam)
			if firstOffset > 0 {
				skip(bf, int64(firstOffset))
			}
		}
		if skipBegin > 0 {
			thisSize := int64(dsLength - (skipBegin + skipEnd))
			if canSeek {
				bf = h5.newSeek(valOffset+skipBegin, thisSize)
			} else {
				skip(bf, int64(firstOffset))
			}
		}
		thisSeg := &segment{
			offset: offset + skipBegin,
			length: dsLength - (skipBegin + skipEnd),
			r:      bf,
		}
		if int64(thisSeg.offset+thisSeg.length) > h5.fileSize {
			if int64(thisSeg.offset) >= h5.fileSize {
				thisSeg.r = makeFillValueReader(obj, nil, int64(thisSeg.length))
			} else {
				length := h5.fileSize - int64(thisSeg.offset)
				rr := newResetReader(thisSeg.r, length)
				thisSeg.r = makeFillValueReader(obj, rr, int64(thisSeg.length))
			}
		}
		segments.append(thisSeg)
		offset += dsLength
	}
	segments.sort()
	readers := make([]io.Reader, 0)
	off := firstOffset
	remOffset := invalidAddress
	logger.Info("firstoffset=", firstOffset, "lastOffset=", lastOffset)
	for i := 0; i < segments.Len(); i++ {
		seg := segments.get(i)
		r := seg.r
		assert(seg.offset <= off, "discontiguous data")
		logger.Infof("Reader at offset 0x%x length %d", seg.offset, seg.length)
		readers = append(readers, newResetReader(r, int64(seg.length)))
		off += seg.length
		remOffset = seg.offset + seg.length
	}
	if size > offset && remOffset != invalidAddress {
		extra := size - offset
		logger.Infof("Fill value reader at end offset 0x%x length %d",
			remOffset-extra, extra)
		readers = append(readers, makeFillValueReader(obj, nil, int64(extra)))
		off += extra
	}
	assertError(off <= lastOffset, ErrCorrupted,
		fmt.Sprintf("this only happens in corrupted files (2) %d %d", off, lastOffset))
	r := newResetReader(io.MultiReader(readers...), int64(size))
	return r
}

// Not including any slice changes
func calcAttrSize(attr *attribute) int64 {
	size := int64(attr.length)
	for _, d := range attr.dimensions {
		size *= int64(d)
	}
	return size
}

func makeFillValueReader(obj *object, bf io.Reader, length int64) io.Reader {
	undefinedFillValue := false
	objFillValue := obj.fillValue
	if obj.fillValue == nil {
		objFillValue = obj.fillValueOld
	}
	if objFillValue != nil {
		if &objFillValue[0] == &fillValueUndefinedConstant[0] {
			logger.Info("Using the undefined fill value")
			undefinedFillValue = true
			objFillValue = nil
		}
	}
	if objFillValue == nil {
		// Set reasonable defaults, then have the individual types override
		if undefinedFillValue {
			objFillValue = []byte{0xff}
		} else {
			objFillValue = []byte{0}
		}
		objFillValue = defaultFillValue(obj.objAttr.class, obj, objFillValue, undefinedFillValue)
	}
	if len(objFillValue) == 0 {
		logger.Error("zero sized fill value")
		objFillValue = []byte{0}
	}
	if bf == nil {
		return newResetReader(internal.NewFillValueReader(objFillValue), length)
	}
	return newResetReader(
		io.MultiReader(bf, internal.NewFillValueReader(objFillValue)),
		length)
}

func (h5 *HDF5) getData(obj *object) interface{} {
	zlibFound := false
	shuffleFound := false
	fletcher32Found := false
	var shuffleParam uint32
	zlibParam := uint32(0)
	for _, val := range obj.filters {
		switch val.kind {
		case filterDeflate:
			zlibFound = true
			if val.cdv != nil {
				checkVal(1, len(val.cdv), "expected at most one zlib param")
				zlibParam = val.cdv[0]
			}
		case filterShuffle:
			shuffleFound = true
			checkVal(1, len(val.cdv), "expected one shuffle param")
			shuffleParam = val.cdv[0]

		case filterFletcher32:
			fletcher32Found = true
		}
	}
	// TODO if !zlibFound && !shuffleFound && !fletcher32Found && isSlice {
	// we can seek first to save time.  Otherwise, it is slow inefficent reading to get to the
	// place we want (or some complicated algorithm).
	bf := h5.newRecordReader(obj, zlibFound, zlibParam, shuffleFound, shuffleParam, fletcher32Found)
	attr := obj.objAttr
	sz := calcAttrSize(attr)
	logger.Info("about to getdataattr rem=", bf.(remReader).Rem(), "size=", sz)
	if sz > bf.(remReader).Rem() {
		length := sz - bf.(remReader).Rem()
		logger.Info("Add fill value reader", length)
		bf = makeFillValueReader(obj, bf, sz)
	}
	var bff io.Reader
	if attr.isSlice {
		assert(attr.lastDim >= attr.firstDim, "bad slice params")
		var chunkSize int64
		switch {
		case len(attr.dimensions) == 0:
			chunkSize = 1
		case attr.dimensions[0] == 0:
			chunkSize = 0
		default:
			chunkSize = sz / int64(attr.dimensions[0])
		}
		sliceSize := chunkSize * (attr.lastDim - attr.firstDim)
		bff = newResetReader(bf, sliceSize)
	} else {
		bff = newResetReader(bf, sz)
	}
	return getDataAttr(h5, h5, bff, *attr)
}

func getDataAttr(hr heapReader, c caster, bf io.Reader, attr attribute) interface{} {
	for i, v := range attr.dimensions {
		logger.Info("dimension", i, "=", v)
	}
	logger.Info("getDataAttr, class", typeNames[attr.class],
		"length", attr.length, "rem", bf.(remReader).Rem(), "dims=", attr.dimensions)
	dimensions := attr.dimensions
	if attr.isSlice {
		nd := make([]uint64, len(dimensions))
		copy(nd, dimensions)
		if len(dimensions) > 0 {
			nd[0] = uint64(attr.lastDim - attr.firstDim)
		}
		dimensions = nd
	}
	return alloc(attr.class, hr, c, bf, &attr, dimensions)
}

func (h5 *HDF5) cast(attr attribute) reflect.Type {
	varName := "junk"
	origNames := map[string]bool{varName: true}
	for name := range h5.registrations {
		origNames[name] = true
	}
	ty := goTypeString(attr.class, h5, varName, &attr, origNames)
	assert(ty != "", "did not calculate go type")
	has := false
	var proto interface{}
	logger.Info("trying to find cast for", ty, len(h5.registrations),
		"class=", typeNames[attr.class])
loop:
	for _, p := range h5.registrations {
		v := reflect.TypeOf(p)
		// Enums are always ints
		found := false
		switch v.Kind() {
		case reflect.Int8, reflect.Uint8:
			found = attr.length == 1
		case reflect.Int16, reflect.Uint16:
			found = attr.length == 2
		case reflect.Int32, reflect.Uint32:
			found = attr.length == 4
		case reflect.Int64, reflect.Uint64:
			found = attr.length == 8
		}
		if found {
			proto = p
			has = true
			break loop
		}
		// Opaques are always arrays of uint8
		if v.Kind() == reflect.Array && v.Elem().Kind() == reflect.Uint8 && v.Len() == int(attr.length) {
			str := fmt.Sprintf("[%d]uint8", v.Len())
			if str == ty {
				proto = p
				has = true
				break
			}
		}
		// Vlen
		if v.Kind() == reflect.Slice {
			elemType := v.Elem().String()
			if strings.Contains(elemType, ".") {
				s := strings.Split(elemType, ".")
				tailType := s[len(s)-1]
				obj, has := h5.getTypeObj(tailType)
				assert(has, fmt.Sprint("couldn't find ", tailType))
				origNames := map[string]bool{}
				origNames[tailType] = true
				elemType = goTypeString(obj.objAttr.class, h5, tailType, obj.objAttr, origNames)
			}
			sig := fmt.Sprintf("[]%s", elemType)
			if sig == ty {
				proto = p
				has = true
				break
			}
		}
		// compound
		if v.Kind() == reflect.Struct && attr.class == typeCompound {
			// All fields must have the same name and type
			fields := []string{}
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				fType := field.Type.String()
				if strings.Contains(fType, ".") {
					s := strings.Split(fType, ".")
					tailType := s[len(s)-1]
					obj, has := h5.getTypeObj(tailType)
					assert(has, fmt.Sprint("couldn't find ", tailType))
					origNames := map[string]bool{}
					origNames[tailType] = true
					fType = goTypeString(obj.objAttr.class, h5, tailType, obj.objAttr, origNames)
				}
				fields = append(fields, fmt.Sprintf("\t%s %s", field.Name, fType))
			}
			inner := strings.Join(fields, "\n")
			sig := fmt.Sprintf("struct {\n%s\n}\n", inner)
			if sig == ty {
				proto = p
				has = true
				break
			}
		}
	}
	if !has {
		return nil
	}
	ptype := reflect.TypeOf(proto)
	return ptype
}

// Attributes returns the global attributes for this group.
func (h5 *HDF5) Attributes() api.AttributeMap {
	// entry point, panic can bubble up
	assert(h5.rootObject != nil, "nil root object")
	h5.sortAttrList(h5.rootObject)
	return h5.getAttributes(h5.rootObject.attrlist)
}

func (h5 *HDF5) hasAddr(addr uint64) bool {
	return h5.addrs[addr]
}

func (h5 *HDF5) findVariable(varName string) *object {
	obj, has := h5.groupObject.children[varName]
	if !has {
		return nil
	}
	logger.Info("Found variable", varName, "group", h5.groupName, "child=", obj.name)
	hasClass := false
	hasCoordinates := false
	hasName := false
	for _, a := range obj.attrlist {
		switch a.name {
		case "CLASS":
			hasClass = true
		case "NAME":
			nameValue := a.value.(string)
			if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
				logger.Info("found name", nameValue)
				hasName = true
			}
		case "_Netcdf4Coordinates":
			logger.Info("Found _Netcdf4Coordinates")
			hasCoordinates = true
		}
	}
	if hasClass && !hasCoordinates && !hasName {
		logger.Info("doesn't have name")
		return nil
	}
	if obj.objAttr.dimensions == nil {
		logger.Infof("variable %s datatype only", obj.name)
		return nil
	}
	return obj
}

func (h5 *HDF5) findGlobalAttrType(attrName string) string {
	for _, attr := range h5.rootObject.attrlist {
		if attr.name != attrName {
			continue
		}
		origNames := map[string]bool{}
		base := cdlTypeString(attr.class, h5, attrName, attr, origNames)
		dims := ""
		if len(attr.dimensions) == 1 && attr.dimensions[0] == 1 {
		} else {
			for i := 0; i < len(attr.dimensions); i++ {
				dims += "(*)"
			}
		}
		return base + dims
	}
	fail("didn't find attribute type")
	panic("silence warning")
}

func (h5 *HDF5) findGlobalAttrGoType(attrName string) string {
	for _, attr := range h5.rootObject.attrlist {
		if attr.name != attrName {
			continue
		}
		origNames := map[string]bool{}
		base := goTypeString(attr.class, h5, attrName, attr, origNames)
		dims := ""
		if len(attr.dimensions) == 1 && attr.dimensions[0] == 1 {
		} else {
			for i := 0; i < len(attr.dimensions); i++ {
				dims += "[]"
			}
		}
		return dims + base
	}
	fail("didn't find attribute type")
	panic("silence warning")
}

func (h5 *HDF5) findType(varName string) string {
	obj := h5.findVariable(varName)
	if obj == nil {
		return ""
	}
	origNames := map[string]bool{varName: true}
	return cdlTypeString(obj.objAttr.class, h5, varName, obj.objAttr, origNames)
}

func (h5 *HDF5) getTypeObj(typeName string) (*object, bool) {
	obj, has := h5.groupObject.children[typeName]
	if !has {
		return nil, false
	}
	logger.Info("Found type", typeName, "group", h5.groupName, "child=", obj.name)
	if obj.objAttr.dimensions != nil {
		logger.Info("this is a variable")
		return nil, false
	}
	if obj.isGroup {
		logger.Info("this is a group")
		return nil, false
	}
	assert(len(obj.attrlist) == 0, "types don't have attributes")
	return obj, true
}

// GetType gets the CDL description of the type and sets the bool to true if found.
func (h5 *HDF5) GetType(typeName string) (string, bool) {
	obj, has := h5.getTypeObj(typeName)
	if !has {
		return "", false
	}
	origNames := map[string]bool{typeName: true}
	sig := cdlTypeString(obj.objAttr.class, h5, typeName, obj.objAttr, origNames)
	return sig, true
}

// GetGoType gets the Go description of the type and sets the bool to true if found.
func (h5 *HDF5) GetGoType(typeName string) (string, bool) {
	obj, has := h5.groupObject.children[typeName]
	if !has {
		return "", false
	}
	logger.Info("Found type", typeName, "group", h5.groupName, "child=", obj.name)
	if len(obj.attrlist) != 0 {
		logger.Info("types don't have attributes")
		return "", false
	}
	if obj.objAttr.dimensions != nil {
		logger.Info("this is a variable")
		return "", false
	}
	if obj.isGroup {
		logger.Info("this is a group")
		return "", false
	}
	origNames := map[string]bool{typeName: true}
	sig := goTypeString(obj.objAttr.class, h5, typeName, obj.objAttr, origNames)
	return fmt.Sprintf("type %s %s", typeName, sig), true
}

// ListTypes returns the user-defined type names.
func (h5 *HDF5) ListTypes() []string {
	var ret []string
	for typeName, obj := range h5.groupObject.children {
		if obj.isGroup {
			continue
		}
		hasClass := false
		hasCoordinates := false
		hasName := false
		for _, a := range obj.attrlist {
			switch a.name {
			case "CLASS":
				hasClass = true
			case "NAME":
				nameValue := a.value.(string)
				if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
					logger.Info("found name", nameValue)
					hasName = true
				}
			case "_Netcdf4Coordinates":
				logger.Info("Found _Netcdf4Coordinates")
				hasCoordinates = true
			}
		}
		if hasClass && !hasCoordinates && !hasName {
			continue
		}
		if obj.objAttr.dimensions != nil {
			// this is a variable
			continue
		}
		ret = append(ret, typeName)
	}
	return ret
}

// ListDimensions lists the names of the dimensions in this group.
func (h5 *HDF5) ListDimensions() []string {
	var ret []string
	children := h5.groupObject.sortChildren()
	for _, obj := range children {
		if obj.isGroup {
			continue
		}
		hasClass := false
		hasCoordinates := false
		hasName := false
		for _, a := range obj.attrlist {
			switch a.name {
			case "CLASS":
				hasClass = true
			case "NAME":
				nameValue := a.value.(string)
				if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
					logger.Info("found name", nameValue)
					hasName = true
				}
			case "_Netcdf4Coordinates":
				logger.Info("Found _Netcdf4Coordinates")
				hasCoordinates = true
			}
		}
		if hasClass && !hasCoordinates && !hasName {
			ret = append(ret, obj.name)
		}
	}
	return ret
}

// GetDimension returns the size of the given dimension and sets
// the bool to true if found.
func (h5 *HDF5) GetDimension(name string) (uint64, bool) {
	for _, obj := range h5.groupObject.children {
		if obj.isGroup {
			continue
		}
		hasClass := false
		hasCoordinates := false
		hasName := false
		for _, a := range obj.attrlist {
			switch a.name {
			case "CLASS":
				hasClass = true
			case "NAME":
				nameValue := a.value.(string)
				if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
					logger.Info("found name", nameValue)
					hasName = true
				}
			case "_Netcdf4Coordinates":
				logger.Info("Found _Netcdf4Coordinates")
				hasCoordinates = true
			}
		}
		if hasClass && !hasCoordinates && !hasName {
			if obj.name == name {
				return obj.objAttr.dimensions[0], true
			}
		}
	}
	return 0, false
}

func (h5 *HDF5) findSignature(signature string, name string, origNames map[string]bool, printer printerType) string {
	if h5.groupObject == nil {
		return ""
	}
	for varName, obj := range h5.groupObject.children {
		if obj.isGroup {
			continue
		}
		if origNames[varName] {
			continue
		}
		hasClass := false
		hasCoordinates := false
		hasName := false
		for _, a := range obj.attrlist {
			switch a.name {
			case "CLASS":
				hasClass = true
			case "NAME":
				nameValue := a.value.(string)
				if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
					logger.Info("found name", nameValue)
					hasName = true
				}
			case "_Netcdf4Coordinates":
				logger.Info("Found _Netcdf4Coordinates")
				hasCoordinates = true
			}
		}
		if hasClass && !hasCoordinates && !hasName {
			continue
		}
		if obj.objAttr.dimensions != nil {
			// this is a variable
			continue
		}
		origNames[varName] = true
		sig := printer(obj.objAttr.class, h5, name, obj.objAttr, origNames)
		origNames[varName] = false
		if sig != "" && sig == signature {
			return obj.name
		}
	}
	return ""
}

func (h5 *HDF5) parseAttr(obj *object, a *attribute) {
	if a.df != nil {
		a.df = makeFillValueReader(obj, a.df, calcAttrSize(a))
		logger.Infof("Reparsing attribute %s %s %p", a.name, typeNames[a.class], a)
		assert(!a.shared, "shared attr unexpected here")
		// very hacky
		save := h5.registrations
		switch a.name {
		case "DIMENSION_LIST", "NAME", "REFERENCE_LIST", "CLASS":
			h5.registrations = nil
		}
		a.value = getDataAttr(h5, h5, a.df, *a)
		a.df = nil
		h5.registrations = save
	}
}

func (h5 *HDF5) getAttributes(unfiltered []*attribute) api.AttributeMap {
	filtered := make(map[string]interface{})
	for i := range unfiltered {
		val := unfiltered[i]
		logger.Infof("getting attribute %s %p", val.name, val)
		switch val.name {
		case "_Netcdf4Dimid", "_Netcdf4Coordinates", "DIMENSION_LIST", "NAME", "REFERENCE_LIST", "CLASS":
			logger.Infof("Found a %v %v %T", val.name, val.value, val.value)
		default:
			if val.value == nil {
				if val.df == nil {
					logger.Info("Need fill value reader", val.name)
					fakeObj := newObject()
					sz := calcAttrSize(val)
					val.df = makeFillValueReader(fakeObj, nil, sz)
				}
				val.value = getDataAttr(h5, h5, val.df, *val)
			}
			value := undoScalarAttribute(val.value)
			filtered[val.name] = value
		}
	}
	keys := []string{}
	for key := range filtered {
		keys = append(keys, key)
	}
	om, err := newTypedAttributeMap(h5, keys, filtered)
	thrower.ThrowIfError(err)
	om.Hide(ncpKey)
	return om
}

// A scalar attribute can be stored as a single-length array.
// This code undoes that to return an actual scalar.
func undoScalarAttribute(value interface{}) interface{} {
	// Opaque is a slice, but we don't want to undo it.
	_, has := value.(opaque)
	if has {
		return value
	}
	_, has = value.(enumerated)
	// Enumerated that haven't been cast can also be slices.
	if has {
		e, has := value.(enumerated)
		if has {
			e.values = undoScalarAttribute(e.values)
			value = e
		}
		return value
	}
	// All other slices are undone
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Slice && v.Len() == 1 {
		v = v.Index(0)
	}
	return v.Interface()
}

// TODO: make this smarter by finding the group first
func findDim(obj *object, oaddr uint64, group string) string {
	prefix := ""
	if len(group) > 0 {
		prefix = group + "/"
	}
	for _, o := range obj.children {
		if o.addr == oaddr {
			logger.Info("dim found", o.name)
			return prefix + o.name
		}
		dim := findDim(o, oaddr, prefix+o.name)
		if dim != "" {
			return dim
		}
	}
	return ""
}

func (h5 *HDF5) getDimensions(obj *object) []string {
	logger.Infof("Getting dimensions addr 0x%x", obj.addr)
	dimNames := make([]string, 0)
	for i := range obj.attrlist {
		h5.parseAttr(obj, obj.attrlist[i])
		a := obj.attrlist[i]
		if a.name != "DIMENSION_LIST" {
			continue
		}
		logger.Infof("DIMENSION_LIST=%T 0x%x", a.value, a.value)
		varLen := a.value.([][]uint64)
		for _, v := range varLen {
			for i, addr := range v {
				// Each dimension in the dimension list points to an object address in the global heap
				// TODO: fix this hack to get full 64-bit addresses
				logger.Infof("dimension list %d 0x%x)", i, addr)
				oaddr := uint64(addr)

				dim := findDim(h5.rootObject, oaddr, "")
				if dim != "" {
					base := path.Base(dim)
					dimNames = append(dimNames, base)
				}
			}
		}
	}
	if len(dimNames) > 0 {
		return dimNames
	}

	var f func(ob *object)
	f = func(ob *object) {
		logger.Infof("obj %s 0x%x", ob.name, ob.addr)
		for i := range ob.attrlist {
			h5.parseAttr(ob, ob.attrlist[i])
			a := ob.attrlist[i]
			if a.name != "REFERENCE_LIST" {
				continue
			}
			logger.Infof("value is %T %v", a.value, a.value)
			for k, v := range a.value.([]compound) {
				vals2 := v
				v0 := vals2[0].Val.(uint64)
				v1 := vals2[1].Val.(int32)
				logger.Infof("single ref %d 0x%x %d %s", k, v0, v1, ob.name)
			}
		}
		for _, o := range ob.children {
			f(o)
		}
	}
	f(h5.rootObject)
	for i := range obj.attrlist {
		h5.parseAttr(obj, obj.attrlist[i])
		a := obj.attrlist[i]
		switch a.name {
		case "NAME":
			nameValue := a.value.(string)
			if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
				return append(dimNames, nameValue)
			}
		}
	}
	return nil
}

// GetVariable returns the named variable or sets the error if not found.
func (h5 *HDF5) GetVariable(varName string) (av *api.Variable, err error) {
	err = ErrInternal
	defer thrower.RecoverError(&err)
	found := h5.findVariable(varName)
	if found == nil {
		logger.Infof("variable %s not found", varName)
		return nil, ErrNotFound
	}
	data := h5.getData(found)
	if data == nil {
		return nil, ErrNotFound
	}
	h5.sortAttrList(found)
	dims := h5.getDimensions(found)
	attrs := h5.getAttributes(found.attrlist)
	return &api.Variable{
			Values:     data,
			Dimensions: dims,
			Attributes: attrs,
		},
		nil
}

// GetVarGetter is an function that returns an interface that allows you to get
// smaller slices of a variable, in case the variable is very large and you want to
// reduce memory usage.
func (h5 *HDF5) GetVarGetter(varName string) (slicer api.VarGetter, err error) {
	defer thrower.RecoverError(&err)
	found := h5.findVariable(varName)
	if found == nil {
		logger.Warnf("variable %s not found", varName)
		return nil, ErrNotFound
	}
	h5.sortAttrList(found)
	d := int64(0)
	fakeEnd := false
	switch {
	case found.objAttr.dimensions == nil:
		d = 1
		fakeEnd = true
	case len(found.objAttr.dimensions) == 0:
		d = 1
		fakeEnd = true
	default:
		d = int64(found.objAttr.dimensions[0])
	}
	getSlice := func(begin, end int64) (interface{}, error) {
		if begin == 0 && end == 1 && fakeEnd {
			data := h5.getData(found)
			if data == nil {
				return nil, ErrNotFound
			}
			return data, nil
		}
		if end < begin {
			return nil, errors.New("invalid slice parameters")
		}
		fakeObj := *found
		fakeObj.objAttr.isSlice = true
		fakeObj.objAttr.firstDim = begin
		fakeObj.objAttr.lastDim = end
		data := h5.getData(&fakeObj)
		if data == nil {
			return nil, ErrNotFound
		}
		return data, nil
	}
	dims := h5.getDimensions(found)
	attrs := h5.getAttributes(found.attrlist)
	origNames := map[string]bool{varName: true}
	ty := cdlTypeString(found.objAttr.class, h5, varName, found.objAttr, origNames)
	origNames = map[string]bool{varName: true}
	goTy := goTypeString(found.objAttr.class, h5, varName, found.objAttr, origNames)
	return internal.NewSlicer(getSlice, d, dims, attrs, ty, goTy), nil
}

// ListSubgroups returns the names of the subgroups of this group.
func (h5 *HDF5) ListSubgroups() []string {
	// entry point
	// Only go one level down
	var ret []string
	var sgDescend func(obj *object, group string)
	sgDescend = func(obj *object, group string) {
		if !obj.isGroup {
			return
		}
		if group != h5.groupName && strings.HasPrefix(group, h5.groupName) {
			// Is a subgroup.  Get the basename of this child.
			tail := group[len(h5.groupName):]
			tail = tail[:len(tail)-1] // trim trailing slash
			assertError(!strings.Contains(tail, "/"), ErrInternal, "trailing slash")
			ret = append(ret, tail)
			return
		}
		obj.sortChildren()
		for _, o := range obj.children {
			sgDescend(o, group+o.name+"/")
		}
	}
	sgDescend(h5.rootObject, "/")
	return ret
}

func (h5 *HDF5) sortAttrList(obj *object) {
	if obj.attrListIsSorted {
		return
	}
	for i := range obj.attrlist {
		h5.parseAttr(obj, obj.attrlist[i])
	}
	sort.Slice(obj.attrlist, func(i, j int) bool {
		return obj.attrlist[i].creationOrder < obj.attrlist[j].creationOrder
	})
	obj.attrListIsSorted = true
}

func (obj *object) sortChildren() []*object {
	var children []*object
	for _, child := range obj.children {
		children = append(children, child)
	}
	sort.Slice(children, func(i, j int) bool {
		return children[i].creationOrder < children[j].creationOrder
	})
	return children
}

// ListVariables lists the variables in this group.
func (h5 *HDF5) ListVariables() []string {
	// entry point, panic can bubble up
	var ret []string
	var descend func(obj *object, group string)
	descend = func(obj *object, group string) {
		children := obj.sortChildren()
		for _, o := range children {
			if group == h5.groupName && o.name != "" {
				hasClass := false
				hasCoordinates := false
				hasName := false
				for _, a := range o.attrlist {
					switch a.name {
					case "CLASS":
						hasClass = true
					case "NAME":
						nameValue := a.value.(string)
						if !strings.HasPrefix(nameValue, "This is a netCDF dimension") {
							logger.Info("found name", nameValue)
							hasName = true
						}
					case "_Netcdf4Coordinates":
						logger.Info("Found _Netcdf4Coordinates")
						hasCoordinates = true
					}
					if hasClass && !hasCoordinates && !hasName {
						logger.Info("skip because", o.name, "is a dimension=", o.objAttr.dimensions)
						continue
					}
				}
				found := h5.findVariable(o.name)
				if found == nil {
					continue
				}
				logger.Info("append", o.name)
				ret = append(ret, o.name)
				continue
			}
			descend(o, group+o.name+"/")
		}
	}
	// TODO: "/" may be overly broad
	descend(h5.rootObject, "/")
	return ret
}

func emptySlice(v interface{}) reflect.Value {
	top := reflect.ValueOf(v)
	elemType := top.Type().Elem()
	slices := 0
	// count how many slices we need to make
	for elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
		slices++
	}
	// here's one slice
	empty := reflect.MakeSlice(reflect.SliceOf(elemType), 0, 0)
	// here are the rest
	for i := 1; i < slices; i++ {
		empty = reflect.MakeSlice(reflect.SliceOf(empty.Type()), 0, 0)
	}
	return empty
}

func undoInterfaces(v interface{}) reflect.Value {
	top := reflect.ValueOf(v)
	if top.Kind() != reflect.Slice {
		return top
	}
	length := reflect.ValueOf(v).Len()
	if length == 0 {
		return emptySlice(v)
	}
	underlying := undoInterfaces(top.Index(0).Interface())
	val := reflect.MakeSlice(reflect.SliceOf(underlying.Type()), length, length)
	val.Index(0).Set(underlying)
	for i := 1; i < val.Len(); i++ {
		underlying = undoInterfaces(top.Index(i).Interface())
		if !underlying.Type().AssignableTo(val.Type().Elem()) {
			return top
		}
		val.Index(i).Set(underlying)
	}
	return val
}

func convert(v interface{}) interface{} {
	val := undoInterfaces(v)
	assert(val.IsValid(), "invalid conversion")
	return val.Interface()
}

func (h5 *HDF5) register(typeName string, proto interface{}) {
	if _, has := h5.GetType(typeName); !has {
		logger.Warn("no such type", typeName)
		return
	}
	h5.registrations[typeName] = proto
}
