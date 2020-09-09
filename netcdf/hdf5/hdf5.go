// Package hdf5 implements HDF5 for NetCDF
package hdf5

import (
	"bufio"
	"bytes"
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
	"github.com/batchatco/go-native-netcdf/netcdf/util"
	"github.com/batchatco/go-thrower"
)

const (
	magic          = "\211HDF\r\n\032\n"
	invalidAddress = ^uint64(0)
	unlimitedSize  = ^uint64(0)
)

// TODO: make these constants "enums" by giving them a type
const (
	// The doc says only early versions of the library use 1,
	// but that is not true.
	dtversionStandard = iota + 1
	dtversionArray
	dtversionPacked
)

// For disabling code

// For some specific things that don't seem to happen
const (
	parseTime          = false
	parseCreationOrder = false
	floatEnums         = false
)

// For some specific things that aren't useful and the code is disabled.
// They are vars so they can be unit tested.
var (
	parseSBExtension     = false // happens, not useful
	parseHeapDirectBlock = false // happens, not useful
	useIndirectBlocks    = false // false until we verify the code
	allowBitfields       = false // not used in NetCDF, but part of HDF5
)

const ncpKey = "_NCProperties"

var (
	ErrBadMagic                = errors.New("bad magic number")
	ErrUnsupportedFilter       = errors.New("unsupported filter found")
	ErrUnknownCompression      = errors.New("unknown compression")
	ErrInternal                = errors.New("internal error")
	ErrNotFound                = errors.New("not found")
	ErrFletcherChecksum        = errors.New("fletcher checksum failure")
	ErrVersion                 = errors.New("hdf5 version not supported")
	ErrLinkType                = errors.New("link type not supported")
	ErrVirtualStorage          = errors.New("virtual storage not supported")
	ErrTruncated               = errors.New("file is too small, may be truncated")
	ErrOffsetSize              = errors.New("only 64-bit offsets are supported")
	ErrDimensionality          = errors.New("invalid dimensionality")
	ErrDataObjectHeaderVersion = errors.New("data object header version not supported")
	ErrDataspaceVersion        = errors.New("dataspace version not supported")
	ErrCorrupted               = errors.New("corrupted file")
	ErrLayout                  = errors.New("data layout version not supported")
	ErrSuperblock              = errors.New("superblock extension not supported")
	ErrIndirectBlocks          = errors.New("indirect blocks not implemented")
	ErrBitfield                = errors.New("bitfields not supported")
	ErrArrays                  = errors.New("arrays not supported")
	ErrExternal                = errors.New("external data files not supported")
	ErrFloatingPoint           = errors.New("non-standard floating point not handled")
	ErrFixedPoint              = errors.New("non-standard fixed-point not handled")
	ErrReference               = errors.New("unsupported reference type")
)

const (
	filterDeflate = iota + 1
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

type attribute struct {
	name          string
	value         interface{}
	class         uint8
	vtType        uint8       // for variable length
	signed        bool        // for fixed-point
	children      []attribute // for variable and compound, TODO also need dimensions
	enumNames     []string
	enumValues    []interface{}
	addr          uint64 // for reference
	length        uint32 // datatype length
	layout        []uint64
	dimensions    []uint64 // for compound
	isSlice       bool
	firstDim      int64 // first dimension if getting slice (fake objects only)
	lastDim       int64 // last dimension if getting slice (fake objects only)
	endian        binary.ByteOrder
	dtversion     uint8
	creationOrder uint64
}

type compoundField interface{}
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

type object struct {
	addr             uint64
	link             *linkInfo
	attr             *linkInfo
	children         map[string]*object
	name             string
	attrlist         []attribute
	dataBlocks       []dataBlock
	filters          []filter
	objAttr          attribute
	fillValue        []byte // takes precedence over old fill value
	fillValueOld     []byte
	isGroup          bool
	creationOrder    uint64
	attrListIsSorted bool
}

var fillValueUndefinedConstant = []byte{0xff} // only the pointer is used

type HDF5 struct {
	fname     string
	fileSize  int64
	file      *raFile
	groupName string // fully-qualified

	rootAddr    uint64
	root        *linkInfo
	attribute   *linkInfo
	rootObject  *object
	groupObject *object
	sharedAttrs map[uint64]*attribute
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

var (
	logger = internal.NewLogger()
	log    = "don't use the log package" // prevents usage of standard log package
)

func init() {
	_ = log // silence warning
	//SetLogLevel(util.LevelInfo)
	//thrower.DisableCatching()
}

func assert(condition bool, msg string) {
	if condition {
		return
	}
	fail(msg)
}

func warnAssert(condition bool, msg string) {
	if condition {
		return
	}
	logger.Warn(msg)
}

func infoAssert(condition bool, msg string) {
	if condition {
		return
	}
	logger.Info(msg)
}

func fail(msg string) {
	logger.Error(msg)
	thrower.Throw(ErrInternal)
}

func assertError(condition bool, err error, msg string) {
	if condition {
		return
	}
	logger.Error(msg)
	thrower.Throw(err)
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
	r := h5.file.seekAt(int64(addr))
	if size == 0 {
		size = h5.fileSize - int64(addr)
	}
	// bufio is faster, but can mask errors
	f := bufio.NewReader(r)
	return newResetReader(f, size)
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
	case 3:
		high := read8(r)
		low := read16(r)
		return uint64(uint32(high)<<16) | uint64(low)
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
	bf := h5.newSeek(0, 0)

	checkMagic(bf, 8, magic)

	version := read8(bf)
	logger.Info("superblock version=", version)
	assert(version <= 3, fmt.Sprintf("bad superblock version: %v", version))
	if version == 3 {
		thrower.Throw(ErrVersion)
	}

	if version < 2 {
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

	if version < 2 {
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
	} else {
		flags := read8(bf)
		if flags != 0 {
			logger.Info("flags ignored: v>=2", flags)
		}
		logger.Infof("file consistency flags=%s", binaryToString(uint64(flags)))
	}

	baseAddress := read64(bf)
	logger.Info("base address=", baseAddress)
	checkVal(0, baseAddress, "only support base address of zero")

	sbExtension := invalidAddress
	if version == 2 {
		sbExtension = read64(bf)
		logger.Infof("superblock extension address=%x", sbExtension)
	} else {
		fsIndexAddr := read64(bf)
		logger.Infof("free-space index address=%x", fsIndexAddr)
		checkVal(invalidAddress, fsIndexAddr, "free-space index address not supported")
	}

	eofAddr := read64(bf)
	logger.Infof("end of file address=%x", eofAddr)
	assertError(eofAddr <= uint64(h5.fileSize),
		ErrTruncated,
		fmt.Sprint("File may be truncated. size=", h5.fileSize, " expected=", eofAddr))

	infoAssert(uint64(h5.fileSize) == eofAddr,
		fmt.Sprint("Junk at end of file ignored. size=", h5.fileSize, " expected=", eofAddr))

	if version == 2 {
		rootAddr := read64(bf)
		logger.Infof("root group object header address=%d", rootAddr)
		h5.rootAddr = rootAddr
	} else {
		driverInfoAddress := read64(bf)
		logger.Infof("driver info address=0x%x", driverInfoAddress)
	}

	if version < 2 {
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
	} else {
		h5.checkChecksum(0, 44)
	}
	if sbExtension != invalidAddress {
		logger.Info("superblock extension not supported")
		if parseSBExtension {
			obj := &object{}
			h5.readDataObjectHeader(obj, sbExtension)
		} else {
			thrower.Throw(ErrSuperblock)
		}
	}
}

func checkMagic(bf io.Reader, len int, magic string) {
	b := make([]byte, len)
	read(bf, b)
	found := string(b)
	assertError(found == magic, ErrBadMagic,
		fmt.Sprintf("bad magic=%q expected=%q", found, magic))
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

func readNullTerminatedName(padding int, bf io.Reader) string {
	var name []byte
	nullFound := false
	for !nullFound {
		b := read8(bf)
		if b == 0 {
			logger.Info("namelen=", len(name))
			nullFound = true
			break
		}
		name = append(name, b)
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

func checkZeroes(bf io.Reader, len int) {
	padBytesCheck(bf, len, false /*don't round*/, logger.Warn)
}

// Assumes it is an Attribute
func (h5 *HDF5) readAttributeDirect(obj *object, addr uint64, offset uint64, length uint16,
	creationOrder uint64) {
	logger.Infof("* addr=0x%x offset=0x%x length=%d", addr, offset, length)
	logger.Info("read Attributes at:", addr+offset)
	bf := h5.newSeek(addr+uint64(offset), int64(length))
	h5.readAttribute(obj, bf, creationOrder)
}

func (h5 *HDF5) printDatatype(obj *object, bf remReader, df remReader, objCount int64, attr *attribute) {
	assert(bf.Rem() >= 8, "short data")
	b0 := read8(bf)
	b1 := read8(bf)
	b2 := read8(bf)
	b3 := read8(bf)
	bitFields := uint32(b1) | (uint32(b2) << 8) | (uint32(b3) << 16)
	dtversion := (b0 >> 4) & 0xf
	dtclass := b0 & 0xf
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
	default:
		fail(fmt.Sprint("datatype version: ", dtversion))
	}
	vtType := uint8(0)
	attr.dtversion = dtversion
	attr.class = dtclass
	attr.length = dtlength
	switch dtclass {
	// TODO: make functions because this is too long
	case typeFixedPoint:
		logger.Info("* fixed-point")
		// Same structure for all versions, no need to check
		byteOrder := bitFields & 0x1
		paddingType := (bitFields >> 1) & 0x3
		signed := (bitFields >> 3) & 0x1
		attr.signed = signed == 0x1
		logger.Infof("byteOrder=%d paddingType=%d, signed=%d", byteOrder, paddingType, signed)
		if byteOrder != 0 {
			attr.endian = binary.BigEndian
		} else {
			attr.endian = binary.LittleEndian
		}
		assertError(paddingType == 0, ErrFixedPoint, "padding must be zero")
		logger.Info("len properties", bf.Rem())
		assert(bf.Rem() > 0, "properties should be here")
		bitOffset := read16(bf)
		bitPrecision := read16(bf)
		logger.Infof("bitOffset=%d bitPrecision=%d blen=%d", bitOffset, bitPrecision,
			bf.Count())
		assertError(bitOffset == 0, ErrFixedPoint, "bit offset must be zero")
		if df == nil {
			logger.Infof("no data")
			break
		}
		if df.Rem() >= int64(dtlength) {
			attr.value = h5.getDataAttr(df, *attr)
		}

	case typeFloatingPoint:
		logger.Info("* floating-point")
		assertError(dtversion == 1, ErrFloatingPoint, "Only support version 1 of float")
		endian := ((bitFields >> 5) & 0x2) | (bitFields & 0x1)
		switch endian {
		case 0:
			attr.endian = binary.LittleEndian
		case 1:
			attr.endian = binary.BigEndian
		default:
			fail(fmt.Sprint("unhandled byte order: ", endian))
		}
		loPad := ((bitFields >> 1) & 0x1) == 0x1
		assertError(!loPad, ErrFloatingPoint, "low pad not supported")
		hiPad := ((bitFields >> 2) & 0x1) == 0x1
		assertError(!hiPad, ErrFloatingPoint, "high pad not supported")
		intPad := ((bitFields >> 3) & 0x1) == 0x1
		assertError(!intPad, ErrFloatingPoint, "internal pad not supported")
		mantissaNormalization := (bitFields >> 4) & 0x3
		logger.Info("* mantissa normalization:", mantissaNormalization)
		sign := (bitFields >> 8) & 0xff
		logger.Info("* sign: ", sign)
		assert(bf.Rem() >= 12,
			fmt.Sprint("Properties need to be at least 12 bytes, was ", bf.Rem()))
		bitOffset := read16(bf)
		bitPrecision := read16(bf)
		exponentLocation := read8(bf)
		exponentSize := read8(bf)
		mantissaLocation := read8(bf)
		mantissaSize := read8(bf)
		exponentBias := read32(bf)

		logger.Infof("* bitOffset=%d bitPrecision=%d exponentLocation=%d exponentSize=%d mantissaLocation=%d mantissaSize=%d exponentBias=%d",
			bitOffset,
			bitPrecision,
			exponentLocation,
			exponentSize,
			mantissaLocation,
			mantissaSize,
			exponentBias)
		assertError(bitOffset == 0, ErrFloatingPoint, "bit offset must be zero")
		assertError(mantissaNormalization == 2, ErrFloatingPoint, "mantissa normalization must be 2")
		switch dtlength {
		case 4:
			assertError(sign == 31, ErrFloatingPoint, "float32 sign location must be 31")
			assertError(bitPrecision == 32, ErrFloatingPoint, "float32 precision must be 32")
			assertError(exponentLocation == 23, ErrFloatingPoint, "float32 exponent location must be 23")
			assertError(exponentSize == 8, ErrFloatingPoint, "float32 exponent size must be 8")
			assertError(exponentBias == 127, ErrFloatingPoint, "float32 exponent bias must be 127")
		case 8:
			assertError(sign == 63, ErrFloatingPoint, "float64 sign location must be 63")
			assertError(bitPrecision == 64, ErrFloatingPoint, "float64 precision must be 64")
			assertError(exponentLocation == 52, ErrFloatingPoint, "float64 exponent location must be 52")
			assertError(exponentSize == 11, ErrFloatingPoint, "float64 exponent size must be 11")
			assertError(exponentBias == 1023, ErrFloatingPoint, "float64 exponent bias must be 1023")
		default:
			logger.Error("bad dtlenth for fp", dtlength)
			thrower.Throw(ErrFloatingPoint)
		}
		if df == nil {
			logger.Infof("no data")
			break
		}
		logger.Info("data len", df.Rem())
		assert(df.Rem() >= int64(dtlength), "floating-point data short")
		attr.value = h5.getDataAttr(df, *attr)

	case typeTime:
		// uncomment the following to enable
		if parseTime {
			logger.Info("time, len(data)=", df.Rem())
			var endian binary.ByteOrder
			if bitFields == 0 {
				endian = binary.LittleEndian
				logger.Info("time little-endian")
			} else {
				endian = binary.BigEndian
				logger.Infof("time big-endian")
			}
			var bp int16
			err := binary.Read(bf, endian, &bp)
			thrower.ThrowIfError(err)
			logger.Info("time bit precision=", bp)
			if df.Rem() > 0 {
				fail("time")
			}
		} else {
			logger.Fatal("time code has never been executed before and does nothing")
		}
	case typeString:
		logger.Info("string")
		checkVal(1, dtversion, "Only support version 1 of string")
		padding := bitFields & 0xf
		set := (bitFields >> 3) & 0xf
		if df == nil {
			logger.Infof("no data")
			break
		}
		b := make([]byte, df.Rem())
		read(df, b)
		logger.Infof("* string padding=%d set=%d b[%s]=%s", padding, set,
			attr.name, getString(b))
		attr.value = getString(b)

	case typeBitField:
		endian := hasFlag8(uint8(bitFields), 0)
		switch endian {
		case false:
			attr.endian = binary.LittleEndian
		case true:
			attr.endian = binary.BigEndian
		}
		loPad := hasFlag8(uint8(bitFields), 1)
		assert(!loPad, "low pad not supported")
		hiPad := hasFlag8(uint8(bitFields), 2)
		assert(!hiPad, "high pad not supported")
		bitOffset := read16(bf)
		checkVal(0, bitOffset, "bit offset must be zero")
		bitPrecision := read16(bf)
		logger.Infof("BitField offset %d, precision %d", bitOffset, bitPrecision)
		if df == nil || df.Rem() == 0 {
			logger.Infof("no data")
			break
		}
		logger.Info("bitfield rem: ", df.Rem())
		if !allowBitfields {
			b := make([]byte, df.Rem())
			read(df, b)
			logger.Infof("bitfield value: %x", b)
			logger.Infof("Bitfields ignored")
			thrower.Throw(ErrBitfield)
		}
		if df.Rem() >= int64(dtlength) {
			attr.value = h5.getDataAttr(df, *attr)
		}

	case typeOpaque:
		if bf.Rem() == 0 {
			logger.Info("No properties for opaque")
			break
		}
		plen := int(bf.Rem())
		tag := make([]byte, plen)
		// not sure what the purpose of the tag is
		read(bf, tag)
		stringTag := getString(tag)
		logger.Info("tag=", stringTag)
		taglen := len(stringTag)
		for i := taglen; i < plen; i++ {
			checkVal(0, tag[i],
				fmt.Sprint("reserved byte should be zero: ", i))
		}
		attr.value = stringTag

	case typeCompound: // compound
		logger.Info("* compound")
		logger.Info("dtversion", dtversion)
		assert(dtversion >= 1 && dtversion <= 3, "compound version")
		nmembers := bitFields & 0xffff
		logger.Info("* number of members:", nmembers)

		padding := 0
		if dtversion < 3 {
			padding = 7
		}
		for i := 0; i < int(nmembers); i++ {
			name := readNullTerminatedName(padding, bf)
			logger.Info("compound name", name)
			logger.Info(i, "compound name=", name)
			var byteOffset uint32
			switch dtversion {
			case 1, 2:
				byteOffset = read32(bf)
				logger.Infof("[32old] byteOffset=0x%x", byteOffset)
			case 3:
				switch {
				case dtlength < 256:
					byteOffset = uint32(read8(bf))
					logger.Infof("[8] byteOffset=0x%x", byteOffset)
				case dtlength < 65536:
					byteOffset = uint32(read16(bf))
					logger.Infof("[16] byteOffset=0x%x", byteOffset)
				case dtlength < 16777216:
					low := uint32(read16(bf))
					high := uint32(read8(bf))
					logger.Infof("low=0x%x high=0x%x\n", low, high)
					byteOffset = low | (high << 16)
					logger.Infof("[24] byteOffset=0x%x", byteOffset)
				default:
					byteOffset = uint32(read32(bf))
					logger.Infof("[32] byteOffset=0x%x", byteOffset)
				}
			}
			var compoundAttribute attribute
			compoundAttribute.name = name
			if dtversion == 1 {
				dimensionality := read8(bf)
				logger.Info("dimensionality", dimensionality)
				checkZeroes(bf, 3)
				perm := read32(bf)
				logger.Info("permutation", perm)
				if perm != 0 {
					logger.Warn("permutation field should be zero")
				}
				reserved := read32(bf)
				checkVal(0, reserved, "reserved dt")
				compoundAttribute.dimensions = make([]uint64, 4)
				for i := 0; i < 4; i++ {
					dsize := read32(bf)
					logger.Info("dimension", i, "size", dsize)
					compoundAttribute.dimensions[i] = uint64(dsize)
				}
				compoundAttribute.dimensions = compoundAttribute.dimensions[:dimensionality]
			}

			rem := int64(0)
			if df != nil {
				rem = df.Rem()
			}
			logger.Infof("%d compound before: len(prop) = %d len(data) = %d", i, bf.Rem(), rem)

			h5.printDatatype(obj, bf, nil, 0, &compoundAttribute)
			rem = int64(0)
			if df != nil {
				rem = df.Rem()
			}
			logger.Infof("%d compound after: len(prop) = %d len(data) = %d", i, bf.Rem(), rem)
			logger.Infof("%d compound dtlength", compoundAttribute.length)
			attr.children = append(attr.children, compoundAttribute)
		}
		logger.Info("Compound length is", attr.length)
		rem := int64(0)
		if df != nil {
			rem = df.Rem()
		}
		if rem > 0 {
			logger.Info("compound alloced", df.Count(), df.Rem()+df.Count())
			bff := makeFillValueReader(obj, df, calcAttrSize(attr))
			attr.value = h5.getDataAttr(bff, *attr)
			logger.Info("rem=", df.Rem(), "nread=", bff.(remReader).Count())
		}
		logger.Info("Finish compound")

	case typeReference:
		logger.Info("* reference")
		checkVal(1, dtversion, "Only support version 1 of reference")
		rType := bitFields & 0xf
		assertError(rType == 0, ErrReference, "rtype must be zero")
		logger.Info("* rtype=object")
		warnAssert((bitFields & ^uint32(0xf)) == 0, "reserved must be zero")
		if df == nil {
			logger.Infof("no data")
			break
		}
		checkVal(8, dtlength, "refs must be 8 bytes")
		bf := newResetReader(df, int64(dtlength))
		addr := read64(bf)
		assert(dtlength <= 8, "weird dtlength")
		logger.Infof("reference addr=0x%x", addr)
		logger.Infof("Setting attr %s to reference", attr.name)
		attr.value = addr
		attr.addr = addr

	case typeEnumerated:
		logger.Info("blen begin", bf.Count())
		var enumAttr attribute
		h5.printDatatype(obj, bf, nil, 0, &enumAttr)
		logger.Info("blen now", bf.Count())
		numberOfMembers := bitFields & 0xffff
		logger.Info("number of members=", numberOfMembers)
		names := make([]string, numberOfMembers)
		padding := 7
		if dtversion == dtversionPacked {
			padding = 0
		}
		for i := uint32(0); i < numberOfMembers; i++ {
			name := readNullTerminatedName(padding, bf)
			names[i] = name
		}
		enumAttr.enumNames = names
		logger.Info("enum names:", names)
		assert(enumAttr.class == typeFixedPoint, "only fixed-point enums supported")
		values := make([]interface{}, numberOfMembers)
		for i := uint32(0); i < numberOfMembers; i++ {
			values[i] = h5.getDataAttr(bf, enumAttr)
			switch values[i].(type) {
			case uint64, int64:
			case uint32, int32:
			case uint16, int16:
			case uint8, int8:
			default:
				// Other enumeration types are not supported in NetCDF
				fail("unknown enumeration type")
			}
		}
		enumAttr.enumValues = values
		logger.Info("enum values:", values)
		attr.children = []attribute{enumAttr}
		if df != nil && df.Rem() > 0 {
			// Read away some bytes
			dff := makeFillValueReader(obj, df, calcAttrSize(attr))
			attr.value = h5.getDataAttr(dff, *attr)
		}

	case typeVariableLength:
		logger.Info("* variable-length, dtlength=", dtlength,
			"proplen=", bf.Rem())
		//checkVal(1, dtversion, "Only support version 1 of variable-length")
		vtType = uint8(bitFields & 0xf) // XXX: we will need other bits too for decoding
		vtPad := uint8(bitFields>>4) & 0xf
		// The value of pad here may not have anything to do with reading data, just
		// writing.  So we could accept all of them
		assert(vtPad == 0 || vtPad == 1, "only do v0 and v1 versions of VL padding")
		vtCset := (bitFields >> 8) & 0xf
		logger.Infof("type=%d paddingtype=%d cset=%d", vtType, vtPad, vtCset)
		switch vtType {
		case 0:
			checkVal(0, vtCset, "cset when not string")
			logger.Infof("sequence")
		case 1:
			if vtCset == 0 {
				logger.Infof("string (ascii)")
			} else {
				logger.Infof("string (utf8)")
			}
		default:
			fail("unknown variable-length type")
		}
		var variableAttr attribute
		h5.printDatatype(obj, bf, nil, 0, &variableAttr)
		logger.Info("variable class", variableAttr.class,
			"vtType", vtType)
		attr.children = append(attr.children, variableAttr)
		attr.vtType = vtType
		rem := int64(0)
		if df != nil {
			rem = df.Rem()
		}
		if rem < int64(dtlength) {
			logger.Infof("variable-length short data: %d vs. %d", rem, dtlength)
			break
		}
		logger.Info("len data is", rem, "dlen", df.Count())

		attr.value = h5.getDataAttr(df, *attr)
		logger.Infof("Type of this vattr: %T", attr.value)

	case typeArray:
		logger.Info("Array")
		dimensionality := read8(bf)
		logger.Info("dimensionality", dimensionality)
		if dtversion < 3 {
			checkZeroes(bf, 3)
		}
		dimensions := make([]uint64, dimensionality)
		for i := 0; i < int(dimensionality); i++ {
			dimensions[i] = uint64(read32(bf))
			logger.Info("dim=", dimensions[i])
		}
		logger.Info("dimensions=", dimensions)
		if dtversion < 3 {
			for i := 0; i < int(dimensionality); i++ {
				perm := read32(bf)
				logger.Info("perm=", perm)
			}
		}
		var arrayAttr attribute
		h5.printDatatype(obj, bf, nil, 0, &arrayAttr)
		arrayAttr.dimensions = dimensions
		attr.children = append(attr.children, arrayAttr)
		if df != nil && df.Rem() > 0 {
			logger.Info("Using an array in an attribute")
			attr.value = h5.getDataAttr(df, *attr)
		}

	default:
		fail(fmt.Sprint("bogus type not handled: ", dtclass))
	}
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
	obj.attrlist = append(obj.attrlist, attribute{name: name})
	attr := &obj.attrlist[len(obj.attrlist)-1]
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
		attr.dimensions = dims
		logger.Info("dimensions are", dims)
		logger.Info("count objects=", count)
	}
	logger.Info("sizeRem=", bf.Rem())
	if !sharedType {
		pf := newResetReaderFromBytes(dtb)
		h5.printDatatype(obj, pf, bf, count, attr)
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
		oa := h5.getSharedAttr(addr)
		sAttr := *oa
		sAttr.dimensions = dims
		attr.value = h5.getDataAttr(bf, sAttr)
	}
}

func (h5 *HDF5) getSharedAttr(addr uint64) *attribute {
	oa := h5.sharedAttrs[addr]
	if oa == nil {
		obj := &object{}
		h5.readDataObjectHeader(obj, addr)
		oa = &obj.objAttr
		h5.sharedAttrs[addr] = oa
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
		if offset < blockSize {
			blockToUse = block
			break
		}
		offset -= blockSize
		row := entryNum / width
		// We double the third row and beyond.
		if row >= 1 && (entryNum%width) == (width-1) {
			logger.Info("doubled block size")
			blockSize *= 2
		}
	}
	if offset < blockSize {
		callback(obj, blockToUse, offset, length, creationOrder)
		return
	}
	if useIndirectBlocks {
		// now try indirect blocks
		logger.Warn("Using indirect blocks")
		for entryNum, block := range link.iBlock {
			if offset < blockSize {
				blockToUse = block
				break
			}
			offset -= blockSize
			if (entryNum % width) == (width - 1) {
				logger.Info("doubled block size")
				blockSize *= 2
			}
		}
		assert(blockToUse != invalidAddress, "did not find direct or indirect block")

		nextLink := *link
		h5.readRootBlock(&nextLink, blockToUse, 0, link.rowsRootIndirect,
			link.tableWidth, blockSize, link.maximumBlockSize)

		h5.readLinkData(obj, link, offset, length, creationOrder, callback)
		return
	} else {
		thrower.Throw(ErrIndirectBlocks)
		panic("never gets here")
	}
}

func (h5 *HDF5) readLinkData(obj *object, link *linkInfo, offset uint64, length uint16,
	creationOrder uint64, callback doublerCallback) {
	logger.Infof("offset=0x%x length=%d", offset, length)
	h5.doDoubling(obj, link, offset, length, creationOrder, callback)
}

func hasFlag8(flags byte, flag uint) bool {
	return (flags>>flag)&0x01 == 0x01
}

// Assumes it is a link
func (h5 *HDF5) readLinkDirect(parent *object, addr uint64, offset uint64, length uint16,
	creationOrder uint64) {
	logger.Infof("* addr=0x%x offset=0x%x length=%d", addr, offset, length)
	bf := h5.newSeek(addr+uint64(offset), int64(length))
	h5.readLinkDirectFrom(parent, bf, length, creationOrder)
}

func (h5 *HDF5) readLinkDirectFrom(parent *object, obf io.Reader, length uint16, creationOrder uint64) {
	//defer SetLogLevel(SetLogLevel(util.LevelInfo))
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
		cSet := read8(bf)
		logger.Info("cset=", cSet)
		assert(cSet == 0 || cSet == 1, "only ASCII and UTF-8 names")
	}
	size := 1 << (flags & 0x3)
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
	if hasAddr(h5.rootObject, hardAddr) {
		logger.Warn("ignore duplicate object")
		logger.Infof("done with name=%s", string(linkName))
		return
	}
	obj := &object{}
	obj.name = string(linkName)
	parent.children[obj.name] = obj
	obj.creationOrder = co
	obj.addr = hardAddr
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
		case 5: //for indexing the ‘name’ field for links in indexed groups.
			logger.Info("Name field for links in indexed groups")
			hash := read32(bf)
			// heap ID
			versionAndType := read8(bf)
			logger.Infof("hash=0x%x versionAndType=%s", hash,
				binaryToString(uint64(versionAndType)))
			idType := (versionAndType >> 4) & 0x3
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
				idType := (versionAndType >> 4) & 0x3
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
			idType := (versionAndType >> 4) & 0x3
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
				idType := (versionAndType >> 4) & 0x3
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
	bf := h5.newSeek(bta, 0) // TODO: figure out length
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
			// This doesn't seem to happen
			logger.Warn("Encountered nodeLevel > 0 for first time")
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
			// TODO: this check is wrong
			logger.Info("1 more byte")
			more := read8(bf)
			blockOffset = blockOffset | (uint64(more) << 32)
		}
		logger.Infof("block offset=0x%x", blockOffset)
		logger.Infof("(block size=%d)", blockSize)
		// TODO: only check checksum if heap flags say so
		// Get checksum before zeroing it out to recalculate it
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

func (h5 *HDF5) readRootBlock(link *linkInfo, bta uint64, flags uint8, nrows uint16, width uint16, startBlockSize uint64, maxBlockSize uint64) {
	bf := h5.newSeek(bta, 0) // TODO: figure out size
	checkMagic(bf, 4, "FHIB")
	version := read8(bf)
	logger.Info("heap root block version=", version)
	heapHeaderAddr := read64(bf)
	logger.Infof("heap header addr=0x%x", heapHeaderAddr)
	blockOffset := uint64(read32(bf))
	logger.Infof("block offset=0x%x", blockOffset)
	logger.Info("max heap size", link.maxHeapSize)
	// sig version heapaddr blockoffset + variables
	len := 4 + 1 + 8 + uint16(link.maxHeapSize/8) + nrows*width*8
	if link.maxHeapSize == 40 {
		logger.Info("1 more byte")
		more := read8(bf)
		blockOffset = blockOffset | (uint64(more) << 32)
	}
	logger.Infof("block offset=0x%x", blockOffset)
	logger.Info("rows width=", nrows, width)
	// TODO: compute K and N
	// should read K values here
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
	blockSizes := make([]uint64, 0, directRows*int(width))
	iAddrs := make([]uint64, 0, indirectRows*int(width))
	blockSize := startBlockSize
	for i := 0; i < int(nrows); i++ {
		if i > 1 {
			logger.Info("doubled block size")
			blockSize *= 2
		}
		for j := 0; j < int(width); j++ {
			childDirectBlockAddress := read64(bf)
			logger.Infof("child direct block address=0x%x", childDirectBlockAddress)
			if i < maxRowsDirect {
				addrs = append(addrs, childDirectBlockAddress)
				blockSizes = append(blockSizes, blockSize)
			} else {
				iAddrs = append(iAddrs, childDirectBlockAddress)
			}
		}
	}

	// TODO: indirect blocks
	logger.Info("Adding indirect heap blocks")
	link.block = addrs
	link.iBlock = iAddrs
	h5.checkChecksum(bta, int(len))

	for i, addr := range addrs {
		if addr != invalidAddress {
			logger.Infof("%d --- parse heap block: 0x%08x %d ---", i, addr, blockSizes[i])
			h5.readHeapDirectBlock(link, addr, flags, blockSizes[i])
		}
	}
	// then read indirect blocks
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

func (h5 *HDF5) readGlobalHeap(heapAddress uint64, index uint32) []byte {
	bf := h5.newSeek(heapAddress, 0) // TODO: figure out size
	checkMagic(bf, 4, "GCOL")
	version := read8(bf)
	checkVal(1, version, "version")
	for i := 0; i < 3; i++ {
		zero := read8(bf)
		checkVal(0, zero, "zero")
	}
	csize := read64(bf) // collection size, including these fields
	csize -= 16
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
			b := make([]byte, osize)
			read(bf, b)
			if hoi == uint16(index) {
				return b
			}
			l := osize
			if l > 8 {
				l = 8
			}
			rem := asize - osize
			skip(bf, int64(rem))
		}
	}
	return nil
}

func (h5 *HDF5) readHeap(link *linkInfo) {
	bf := h5.newSeek(link.heapAddress, 0) // TODO: figure out size
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
		h5.readRootBlock(link, rootBlockAddress, flags, rowsRootIndirect,
			tableWidth, startingBlockSize, maximumBlockSize)
	} else {
		logger.Info("Adding direct heap block")
		assert(link.block == nil, "don't overwrite direct heap block")
		link.block = make([]uint64, 1)
		link.block[0] = rootBlockAddress
		h5.readHeapDirectBlock(link, rootBlockAddress, flags, startingBlockSize)
	}
}

func (h5 *HDF5) readLocalHeap(addr uint64, offset uint64) string {
	bf := h5.newSeek(addr, 0)
	checkMagic(bf, 4, "HEAP")
	version := read8(bf)
	checkVal(0, version, "version 0 expected for local heap")
	reserved := readEnc(bf, 3)
	logger.Infof("reserved=0x%x", reserved)
	checkVal(0, reserved, "reserved must be zero")
	dsSize := read64(bf)
	flOffset := read64(bf)
	dsAddr := read64(bf)
	logger.Infof("dsSize=%d flOffset=0x%x dsAddr=0x%x", dsSize, flOffset, dsAddr)
	bff := h5.newSeek(dsAddr+offset, int64(dsSize)-int64(offset))
	return readNullTerminatedName(0, bff)
}

func (h5 *HDF5) readSymbolTableLeaf(parent *object, addr uint64, size uint64, heapAddr uint64) {
	bf := h5.newSeek(addr, int64(size))
	checkMagic(bf, 4, "SNOD")
	version := read8(bf)
	checkVal(1, version, "version 1 expected for symbol table leaf")
	reserved := read8(bf)
	checkVal(0, reserved, "reserved must be zero")
	numSymbols := read16(bf)
	used := bf.Count()
	thisSize := 8 * int64(size)
	bf = h5.newSeek(addr+uint64(used), thisSize-used)
	logger.Info("number of symbols", numSymbols)
	for i := 0; i < int(numSymbols); i++ {
		logger.Info("Start: count=", bf.Count(), "rem=", bf.Rem())
		if bf.Rem() < 24 {
			logger.Info(i, "not enough space to read another entry", bf.Rem())
			break
		}
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
			if bf.Rem() < 16 {
				rem = int(bf.Rem())
			}
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
		if hasAddr(h5.rootObject, objectHeaderAddress) {
			logger.Warn("ignore duplicate object")
			logger.Infof("done with name=%s", string(linkName))
			return
		}
		obj := &object{}
		obj.addr = objectHeaderAddress

		obj.name = linkName
		//obj.creationOrder = co
		//logger.Info("obj name", obj.name)
		logger.Infof("object (0x%x, %s) from symbol table, parent (0x%x, %s)\n",
			obj.addr, obj.name, parent.addr, parent.name)
		parent.children[obj.name] = obj
		obj.isGroup = true
		h5.readDataObjectHeader(obj, objectHeaderAddress)
		logger.Infof("STE=%+v rem=%v", obj, bf.Rem())
		//b := make([]byte, bf.Rem())
		//read(bf, b)
		//logger.Infof("rembytes=0x%x", b)
		h5.dumpObject(obj)
		logger.Infof("done with name=%s", obj.name)
	}
}

func (h5 *HDF5) readSymbolTable(parent *object, addr uint64, heapAddr uint64) {
	bf := h5.newSeek(addr, 0) // TODO: figure out size

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
	if leftAddress != invalidAddress || rightAddress != invalidAddress {
		fail("Siblings unexpected")
	}
	assert(nodeType == 0, "what we expect")
	type keyAddr struct {
		key  uint64
		addr uint64
	}
	keyAddrs := []keyAddr{}
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
	for i, v := range keyAddrs {
		if prevAddr != invalidAddress {
			logger.Infof("%d: key=%d prevKey=%d size=%d addr=0x%x", i,
				v.key, prevKey, v.key-prevKey, prevAddr)
			if lastKey > prevKey {
				h5.readSymbolTableLeaf(parent, prevAddr, (v.key - prevKey), heapAddr)
			}
		}
		prevKey = v.key
		prevAddr = v.addr
	}
	if prevAddr != invalidAddress {
		logger.Infof("last: key=%d prevKey=%d size=%d addr=0x%x",
			lastKey, prevKey, lastKey-prevKey, prevAddr)
		if lastKey > prevKey {
			h5.readSymbolTableLeaf(parent, prevAddr, (lastKey - prevKey), heapAddr)
		}
	}
}

func (h5 *HDF5) readBTree(parent *object, addr uint64) {
	bf := h5.newSeek(addr, 0) // TODO: figure out size
	checkMagic(bf, 4, "BTHD")
	version := read8(bf)
	logger.Info("btree version=", version)
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
		for i := 0; i < int(d); i++ {
			pi := read64(bf)
			logger.Infof("dataspace permutation index %d/%d = %d", i, d, pi)
		}
		fail("permutation indices not supported")
	}
	if bf.Rem() > 0 {
		logger.Error("Junk bytes: ", bf.Rem())
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
	assertError(version == 3 || version == 4,
		ErrLayout, fmt.Sprint("unsupported layout version: ", version))
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
				// Single-chunk indexing, has not been seen in the wild.
				fchunksize := read64(bf)
				logger.Info("chunk size = ", fchunksize)
				filters := read32(bf)
				logger.Info("filters = ", filters)
				fail("single chunk indexing not supported")
			case 2:
				logger.Info("implicit indexing")
			case 3:
				pageBits := read8(bf)
				logger.Info("fixed array pagebits=", pageBits)
			case 4:
				// Extensible-array indexing, has not been seen in the wild.
				maxbits := read8(bf)
				indexElements := read8(bf)
				minPointers := read8(bf)
				minElements := read8(bf)
				pageBits := read8(bf) // doc says 16-bit, but is wrong
				logger.Info("extensible array mb=", maxbits,
					"ie=", indexElements, "mp=", minPointers, "me=", minElements,
					"pb=", pageBits)
				fail("extensibile array indexing not supported")
			case 5:
				// Version 2 B-tree indexing, has not been seen in the wild.
				nodeSize := read32(bf)
				splitPercent := read8(bf)
				mergePercent := read8(bf)
				logger.Info("b-tree indexing size=", nodeSize, "split%=", splitPercent, "merge%=", mergePercent)
				fail("Version 2 B-tree array indexing not supported")
			}
			rem := bf.Rem()
			var address uint64
			switch rem {
			case 8:
				address = read64(bf)
				logger.Infof("v4 address=0x%x", address)
				rem -= 8
				if rem > 0 {
					b := make([]byte, rem)
					read(bf, b)
					logger.Warnf("%d bytes remaining (not used): %v", rem, b)
				}
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
		spaceAllocationTime = flags & 0x3
		fillValueWriteTime = (flags >> 2) & 0x3
		fillValueUnDefined = (flags >> 4) & 0x1
		fillValueDefined = (flags >> 5) & 0x1
		reserved := (flags >> 6) & 0x3
		checkVal(0, reserved, "extra bits in fill value")
		if fillValueUnDefined == 0x1 {
			// fillValueUndefined never seems to be set
			logger.Warn("executing fill value undefined code for first time")
			if fillValueDefined == 0x1 {
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

func (h5 *HDF5) readDatatype(obj *object, bf io.Reader) attribute {
	size := bf.(remReader).Rem()
	logger.Infof("going to read %v bytes", size)
	logger.Info("print datatype with properties from chunk")
	var objAttr attribute
	pf := newResetReader(bf, bf.(remReader).Rem())
	h5.printDatatype(obj, pf, nil, 0, &objAttr)
	return objAttr
}

func (h5 *HDF5) readCommon(obj *object, obf io.Reader, version uint8, ohFlags byte, origAddr uint64, chunkSize uint64) {
	//defer SetLogLevel(SetLogLevel(util.LevelInfo))
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
			//assert(int64(size) <= bf.Rem(), "not enough space")
			if int64(size) > bf.Rem() {
				logger.Error("not enough space") // XXX, debugging hack
				return
			}
		}
		if version > 1 {
			nReadSave = bf.Count()
		}
		assert(uint64(size) <= (chunkSize-uint64(nReadSave)),
			fmt.Sprint("too big: ", size, chunkSize, nReadSave))
		if hasFlag8(hFlags, 1) {
			//var d = make([]byte, size)
			//read(bf, d)
			length := read16(bf)
			logger.Info("shared message length", length)
			addr := read64(bf)
			logger.Infof("shared message addr = 0x%x", addr)
			oa := h5.getSharedAttr(addr)
			obj.objAttr = *oa
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
			obj.objAttr = h5.readDatatype(obj, f)
			h5.sharedAttrs[obj.addr] = &obj.objAttr
			logger.Info("before, after", save, obj.objAttr.dimensions)
			obj.objAttr.dimensions = save
			logger.Info("dimensions are", obj.objAttr.dimensions)

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
			comment := readNullTerminatedName(0, f)
			logger.Info("Comment=", comment)

		case typeObjectModificationTimeOld:
			get := func(size int) string {
				b := make([]byte, size)
				read(f, b)
				return string(b)
			}
			year := get(4)    // 4
			month := get(2)   //6
			day := get(2)     //8
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
			logger.Warnf("Unknown header type 0x%x data=%x", headerType, b)
		}
		logger.Info("mid chunksize", chunkSize, "nRead", bf.Count(), "rem",
			bf.Rem())
		rem := f.Rem()
		if rem > 0 {
			if rem < 8 {
				// allowed for padding up to 8-byte boundary
				checkZeroes(f, int(rem))
			} else {
				switch headerType {
				case typeDatatype:
					// This happens with compound data, for as yet unknown reasons.
					logger.Infof("%d junk bytes at end of record type=%s", rem,
						headerTypeToString(int(headerType)))
					checkZeroes(f, int(rem))
				default:
					logger.Errorf("%d junk bytes at end of record type=%s", rem,
						headerTypeToString(int(headerType)))
					checkZeroes(f, int(rem))
					fail("junk")
				}
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
	logger.Infof("read object header %x", addr)
	bf := h5.newSeek(addr, 0) // TODO: figure out size
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
		// These don't matter for read-only.
		logger.Warn("this code has not been reached before")
		s := read16(bf)
		logger.Info("max compact=", s)
		s = read16(bf)
		logger.Info("max dense=", s)
	}

	// Bits 0-1 of the flags determine the size of the first chunk
	nBytesInChunkSize := 1 << (ohFlags & 0x3)
	chunkSize := readEnc(bf, uint8(nBytesInChunkSize))

	newOffset := addr + uint64(bf.Count())

	// Read fields that object header and continuation blocks have in common
	logger.Info("size of chunk=", chunkSize)
	obj.children = make(map[string]*object)
	start := bf.Count()
	h5.readCommon(obj, bf, version, ohFlags, newOffset, chunkSize)
	used := bf.Count() - start
	assert(used == int64(chunkSize),
		fmt.Sprintf("readCommon should read %d bytes, read %d, delta %d",
			chunkSize, used, int64(chunkSize)-used))

	// 	logger.Info("done reading chunks")

	// Finally, compute the checksum
	//	assert(int64(nRead) == cbf.Count(),
	//		fmt.Sprintf("nread not matching count: %v %v", nRead, cbf.Count()))
	h5.checkChecksum(addr, int(bf.Count()))
	logger.Infof("obj %s at addr 0x%x\n", obj.name, obj.addr)
}

func (h5 *HDF5) readDataObjectHeaderV1(obj *object, addr uint64) {
	obj.addr = addr
	logger.Infof("v1 addr=0x%x", addr)
	bf := h5.newSeek(addr, 0) // TODO: figure out size
	version := read8(bf)
	logger.Info("v1 object header version=", version)
	assertError(version == 1, ErrDataObjectHeaderVersion,
		fmt.Sprint("only handle version 1, got: ", version))

	reserved := read8(bf)
	checkVal(0, reserved, "reserved")

	numMessages := read16(bf)
	referenceCount := read32(bf)
	headerSize := read32(bf)
	logger.Info("Num messages", numMessages, "reference count", referenceCount,
		"header size", headerSize)

	// Read fields that object header and continuation blocks have in common
	obj.children = make(map[string]*object)
	count := bf.Count()
	logger.Info("count = ", count)
	rounded := (count + 7) & ^int64(7)
	padding := rounded - count
	if padding > 0 {
		checkZeroes(bf, int(padding))
	}
	h5.readCommon(obj, bf, version, 0, addr+uint64(bf.Count()), uint64(headerSize))
	logger.Info("done reading chunks")
}

func (h5 *HDF5) Close() {
	if h5.file != nil {
		h5.file.Close()
	}
	h5.file = nil
}

func (h5 *HDF5) GetGroup(group string) (g api.Group, err error) {
	defer thrower.RecoverError(&err)
	var groupName string
	switch {
	case strings.HasPrefix(group, "/"):
		// Absolute path
		groupName = group
		if !strings.HasSuffix(groupName, "/") {
			groupName = groupName + "/"
		}
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
		for _, o := range obj.children {
			ret := sgDescend(o, group+o.name+"/")
			if ret != nil {
				return ret
			}
		}
		return nil
	}

	o := sgDescend(h5.rootObject, "/")
	assert(o != nil, fmt.Sprintf("Did not find group %s in %s", group, h5.groupName))

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
	}
	h5 := &HDF5{
		fname:       fname,
		fileSize:    fileSize,
		groupName:   "/",
		file:        newRaFile(file),
		rootAddr:    0,
		root:        nil,
		attribute:   nil,
		rootObject:  nil,
		groupObject: nil,
		sharedAttrs: make(map[uint64]*attribute)}
	h5.readSuperblock()
	assert(h5.rootAddr != invalidAddress, "No root address")
	h5.rootObject = &object{}
	h5.readDataObjectHeader(h5.rootObject, h5.rootAddr)
	h5.groupObject = h5.rootObject
	h5.groupObject.isGroup = true
	h5.dumpObject(h5.rootObject)
	return api.Group(h5), nil
}

func (h5 *HDF5) dumpObject(obj *object) {
	// attributes first
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

func allocInt8s(bf io.Reader, dimLengths []uint64, signed bool) interface{} {
	if len(dimLengths) == 0 {
		value := read8(bf)
		if signed {
			return int8(value)
		}
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		var values interface{}
		if signed {
			values = make([]int8, thisDim)
		} else {
			values = make([]uint8, thisDim)
		}
		err := binary.Read(bf, binary.LittleEndian, values)
		thrower.ThrowIfError(err)
		return values
	}
	var ty reflect.Type
	if signed {
		ty = reflect.TypeOf(int8(0))
	} else {
		ty = reflect.TypeOf(uint8(0))
	}
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocInt8s(bf, dimLengths[1:], signed)))
	}
	return vals.Interface()
}

func allocShorts(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder, signed bool) interface{} {
	if len(dimLengths) == 0 {
		var value uint16
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		if signed {
			return int16(value)
		}
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		var values interface{}
		if signed {
			values = make([]int16, thisDim)
		} else {
			values = make([]uint16, thisDim)
		}
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	var ty reflect.Type
	if signed {
		ty = reflect.TypeOf(int16(0))
	} else {
		ty = reflect.TypeOf(uint16(0))
	}
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocShorts(bf, dimLengths[1:], endian, signed)))
	}
	return vals.Interface()
}

func allocInts(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder, signed bool) interface{} {
	if len(dimLengths) == 0 {
		var value uint32
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		logger.Infof("int=%d (0x%x)", value, value)
		if signed {
			return int32(value)
		}
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		var values interface{}
		if signed {
			values = make([]int32, thisDim)
		} else {
			values = make([]uint32, thisDim)
		}
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	var ty reflect.Type
	if signed {
		ty = reflect.TypeOf(int32(0))
	} else {
		ty = reflect.TypeOf(uint32(0))
	}
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocInts(bf, dimLengths[1:], endian, signed)))
	}
	return vals.Interface()
}

func allocInt64s(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder, signed bool) interface{} {
	if len(dimLengths) == 0 {
		var value uint64
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		if signed {
			return int64(value)
		}
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		var values interface{}
		if signed {
			values = make([]int64, thisDim)
		} else {
			values = make([]uint64, thisDim)
		}
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	var ty reflect.Type
	if signed {
		ty = reflect.TypeOf(int64(0))
	} else {
		ty = reflect.TypeOf(uint64(0))
	}
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocInt64s(bf, dimLengths[1:], endian, signed)))
	}
	return vals.Interface()
}

func allocOpaque(bf io.Reader, dimLengths []uint64, length uint32) interface{} {
	if len(dimLengths) == 0 {
		b := make([]byte, length)
		read(bf, b)
		return opaque(b)
	}
	thisDim := dimLengths[0]
	ty := reflect.TypeOf(opaque{})
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		val := allocOpaque(bf, dimLengths[1:], length)
		vals.Index(int(i)).Set(reflect.ValueOf(val))
	}
	return vals.Interface()
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

func allocFloats(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder) interface{} {
	if len(dimLengths) == 0 {
		var value float32
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]float32, thisDim)
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(reflect.TypeOf(float32(0)), dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocFloats(bf, dimLengths[1:], endian)))
	}
	return vals.Interface()
}

func allocDoubles(bf io.Reader, dimLengths []uint64, endian binary.ByteOrder) interface{} {
	if len(dimLengths) == 0 {
		var value float64
		err := binary.Read(bf, endian, &value)
		thrower.ThrowIfError(err)
		return value
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]float64, thisDim)
		err := binary.Read(bf, endian, values)
		thrower.ThrowIfError(err)
		return values
	}
	vals := makeSlices(reflect.TypeOf(float64(0)), dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(allocDoubles(bf, dimLengths[1:], endian)))
	}
	return vals.Interface()
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

func padBytes(bf io.Reader, pad32 int) {
	padBytesCheck(bf, pad32, true /*round*/, logger.Fatal)
}

func (h5 *HDF5) allocCompounds(bf io.Reader, dimLengths []uint64, attr attribute) interface{} {
	length := attr.length
	cbf := getCountedReader(bf, int64(length))
	logger.Info("count before reset", bf.(remReader).Count(), "rem=", bf.(remReader).Rem())
	//cbf := newResetReader(bf, int64(length) /*+36*/)
	class := typeNames[attr.class]

	logger.Info(cbf.Count(), "Alloc compounds", dimLengths, class, "length=", length,
		"nchildren=", len(attr.children), "rem=", bf.(remReader).Rem())
	dtlen := uint32(0)
	for i := range attr.children {
		dtlen += attr.children[i].length
	}
	packed := false
	if dtlen == length {
		logger.Info("packed", "dtversion=", attr.dtversion)
		packed = true
	} else {
		logger.Info("Not packed", length, dtlen, "dtversion=", attr.dtversion)
	}
	if len(dimLengths) == 0 {
		varray := make([]compoundField, len(attr.children))
		maxPad := 0
		logger.Info("Start length", length)
		for i := range attr.children {
			pad := 0
			logger.Info(cbf.Count(), cbf.Rem(), "Alloc compound child length",
				attr.children[i].name,
				dimLengths,
				attr.children[i].length, "rem=", cbf.Rem())
			switch attr.children[i].class {
			case typeFixedPoint, typeFloatingPoint:
				switch attr.children[i].length {
				case 1: // no padding required
				case 2:
					pad = 1
				case 4:
					pad = 3
				case 8:
					pad = 7
				default:
					fail(fmt.Sprint("compound: bad length: ", attr.children[0].length))
				}
			case typeVariableLength:
				pad = 7
			}
			if pad > 0 && !packed {
				// With compression, there can be junk in the padding
				if !padBytesCheck(cbf, pad, true /*round*/, logger.Info) {
					logger.Info("1. padbytes problem, file:", h5.fname, "pad=", pad)
				}
				if pad > maxPad {
					maxPad = pad
				}
			}
			varray[i] = h5.getDataAttr(cbf, attr.children[i])
		}
		logger.Info(cbf.Count(), "dtlen=", dtlen, "length=", length)
		if maxPad > 0 && !packed {
			// TODO: we compute maxPad, but don't use it (just any pad causes a tail pad of 7).
			// TODO: figure out if this is correct.
			logger.Info("maxpad", maxPad, "count=", cbf.Count())
			if !padBytesCheck(cbf, maxPad, true /*round*/, logger.Info) {
				logger.Info("3. padbytes problem, file:", h5.fname, "pad=", maxPad)
			}
			//padBytes(cbf, maxPad)
		}
		return compound(varray)
	}
	var x compound
	t := reflect.TypeOf(x)
	vals2 := makeSlices(t, dimLengths)
	thisDim := dimLengths[0]
	for i := uint64(0); i < thisDim; i++ {
		vals2.Index(int(i)).Set(reflect.ValueOf(h5.allocCompounds(cbf, dimLengths[1:], attr)))
	}
	logger.Infof("Return val type %T", vals2.Interface())
	return vals2.Interface()
}

func (h5 *HDF5) allocVariable(bf io.Reader, dimLengths []uint64, attr attribute) interface{} {
	logger.Info("allocVariable", dimLengths, "count=", bf.(remReader).Count())
	if len(dimLengths) == 0 {
		var length uint32
		var addr uint64
		var index uint32
		err := binary.Read(bf, binary.LittleEndian, &length)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &addr)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &index)
		thrower.ThrowIfError(err)
		logger.Infof("length %d(0x%x) addr 0x%x index %d(0x%x)\n",
			length, length, addr, index, index)
		var val0 interface{}
		var s []byte
		var bff remReader
		if length == 0 {
			// If there's no value to read, we fake one to get the type.
			attr.dimensions = nil
			s = make([]byte, attr.length)
		} else {
			s = h5.readGlobalHeap(addr, index)
		}
		bff = newResetReaderFromBytes(s)
		val0 = h5.getDataAttr(bff, attr)
		t := reflect.ValueOf(val0).Type()
		sl := reflect.MakeSlice(reflect.SliceOf(t), int(length), int(length))
		if length > 0 {
			sl.Index(0).Set(reflect.ValueOf(val0))
			for i := 1; i < int(length); i++ {
				val := h5.getDataAttr(bff, attr)
				sl.Index(i).Set(reflect.ValueOf(val))
			}
		}
		return sl.Interface()
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		// For scalars, this can be faster using binary.Read
		vals := make([]interface{}, thisDim)
		for i := uint64(0); i < thisDim; i++ {
			logger.Info("Alloc inner", i, "of", thisDim)
			vals[i] = h5.allocVariable(bf, dimLengths[1:], attr)
		}
		assert(vals[0] != nil, "we never return nil")
		t := reflect.ValueOf(vals[0]).Type()
		vals2 := reflect.MakeSlice(reflect.SliceOf(t), int(thisDim), int(thisDim))
		for i := 0; i < int(thisDim); i++ {
			vals2.Index(i).Set(reflect.ValueOf(vals[i]))
		}
		logger.Infof("Return val type %T", vals2.Interface())
		return vals2.Interface()
	}

	// TODO: we sometimes know the type (float32) and can do something smarter here
	vals := make([]interface{}, thisDim)
	for i := uint64(0); i < thisDim; i++ {
		logger.Info("Alloc outer", i, "of", thisDim)
		vals[i] = h5.allocVariable(bf, dimLengths[1:], attr)
	}
	t := reflect.ValueOf(vals[0]).Type()
	vals2 := reflect.MakeSlice(reflect.SliceOf(t), int(thisDim), int(thisDim))
	for i := 0; i < int(thisDim); i++ {
		vals2.Index(i).Set(reflect.ValueOf(vals[i]))
	}
	logger.Infof("Return val type %T", vals2.Interface())
	return vals2.Interface()
}

// Regular strings are fixed length, as opposed to variable length ones
func (h5 *HDF5) allocRegularStrings(bf io.Reader, dimLengths []uint64) interface{} {
	if len(dimLengths) == 0 {
		// maybe a string scalar is just one character?
		b := make([]byte, 1)
		read(bf, b)
		logger.Info("string is", string(b))
		return string(b)
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		b := make([]byte, thisDim)
		read(bf, b)
		logger.Info("string is", string(b))
		return string(b)
	}
	vals := makeStringSlices(dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(h5.allocRegularStrings(bf, dimLengths[1:])))
	}
	return vals.Interface()
}

func (h5 *HDF5) allocReferences(bf io.Reader, dimLengths []uint64) interface{} {
	if len(dimLengths) == 0 {
		var addr uint64
		err := binary.Read(bf, binary.LittleEndian, &addr)
		thrower.ThrowIfError(err)
		logger.Infof("Reference addr 0x%x", addr)
		return int64(addr)
	}

	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]int64, thisDim)
		for i := range values {
			var addr uint64
			err := binary.Read(bf, binary.LittleEndian, &addr)
			thrower.ThrowIfError(err)
			logger.Infof("Reference addr 0x%x", addr)
			values[i] = int64(addr)
		}
		return values
	}
	vals := makeSlices(reflect.TypeOf(int64(0)), dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(h5.allocReferences(bf, dimLengths[1:])))
	}
	return vals.Interface()
}

func (h5 *HDF5) allocStrings(bf io.Reader, dimLengths []uint64) interface{} {
	if len(dimLengths) == 0 {
		// alloc one scalar
		var length uint32
		var addr uint64
		var index uint32

		var err error
		err = binary.Read(bf, binary.LittleEndian, &length)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &addr)
		thrower.ThrowIfError(err)
		err = binary.Read(bf, binary.LittleEndian, &index)
		thrower.ThrowIfError(err)
		logger.Infof("String length %d (0x%x), addr 0x%x, index %d (0x%x)",
			length, length, addr, index, index)
		if length == 0 {
			return ""
		}
		s := h5.readGlobalHeap(addr, uint32(index))
		logger.Info("string=", string(s))
		return getString(s) // TODO: should be s[:length]
	}
	thisDim := dimLengths[0]
	if len(dimLengths) == 1 {
		values := make([]string, thisDim)
		for i := uint64(0); i < thisDim; i++ {
			var length uint32
			var addr uint64
			var index uint32

			err := binary.Read(bf, binary.LittleEndian, &length)
			thrower.ThrowIfError(err)
			err = binary.Read(bf, binary.LittleEndian, &addr)
			thrower.ThrowIfError(err)
			err = binary.Read(bf, binary.LittleEndian, &index)
			thrower.ThrowIfError(err)
			logger.Infof("String length %d (0x%x), addr 0x%x, index %d (0x%x)",
				length, length, addr, index, index)
			if length == 0 {
				values[i] = ""
				continue
			}
			s := h5.readGlobalHeap(addr, index)
			values[i] = getString(s) // TODO: should be s[:length]
		}
		return values
	}
	ty := reflect.TypeOf("")
	vals := makeSlices(ty, dimLengths)
	for i := uint64(0); i < thisDim; i++ {
		vals.Index(int(i)).Set(reflect.ValueOf(h5.allocStrings(bf, dimLengths[1:])))
	}
	return vals.Interface()
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

type unshuffleReader struct {
	r            io.Reader
	b            []byte
	size         uint64
	shuffleParam uint32
}

func newUnshuffleReader(r io.Reader, size uint64, shuffleParam uint32) remReader {
	return newResetReader(&unshuffleReader{r, nil, size, shuffleParam}, int64(size))
}

func unshuffle(val []byte, n uint32) {
	if n == 1 {
		return // avoids allocation
	}
	// inefficent algorithm because it allocates data
	tmp := make([]byte, len(val))
	nelems := len(val) / int(n)
	for i := 0; i < int(n); i++ {
		for j := 0; j < nelems; j++ {
			tmp[j*int(n)+i] = val[i*nelems+j]
		}
	}
	copy(val, tmp)
}

type remReader interface {
	io.Reader
	Count() int64
	Rem() int64
}

func (r *unshuffleReader) Read(p []byte) (int, error) {
	if r.size == 0 {
		return 0, io.EOF
	}
	thisLen := uint64(len(p))
	if thisLen > r.size {
		thisLen = r.size
	}
	var err error
	if r.b == nil {
		r.b = make([]byte, r.size)
		tot, err := readAll(r.r, r.b)
		unshuffle(r.b[:tot], r.shuffleParam)
		if err != nil {
			assert(err == io.EOF, err.Error())
		}
	}
	copy(p, r.b[:thisLen])
	r.b = r.b[thisLen:]
	r.size -= thisLen
	if r.size == 0 {
		return int(thisLen), io.EOF
	}
	return int(thisLen), err
}

func newFletcher32Reader(r io.Reader, size uint64) remReader {
	assert(size >= 4, "bad size for fletcher")
	assert(size%2 != 1, "bad mod for fletcher")
	b := make([]byte, size-4)
	read(r, b)
	var checksum uint32
	binary.Read(r, binary.LittleEndian, &checksum)
	bf := newResetReaderFromBytes(b)
	values := make([]uint16, len(b)/2)
	binary.Read(bf, binary.BigEndian, values)
	calcedSum := fletcher32(values)
	if calcedSum != checksum {
		logger.Error("calced sum=", calcedSum, "file sum=", checksum)
		thrower.Throw(ErrFletcherChecksum)
	}
	return newResetReaderFromBytes(b)
}

type segment struct {
	offset uint64
	length uint64
	r      io.Reader
}

type segments []*segment

func (s segments) Len() int      { return len(s) }
func (s segments) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type byOffset struct{ segments }

func (s byOffset) Less(i, j int) bool {
	if s.segments[i].offset < s.segments[j].offset {
		return true
	}
	if s.segments[i].offset > s.segments[j].offset {
		return false
	}
	return true
}

func (h5 *HDF5) newRecordReader(obj *object, zlibFound bool, zlibParam uint32,
	shuffleFound bool, shuffleParam uint32, fletcher32Found bool) (io.Reader, uint64) {
	nBlocks := len(obj.dataBlocks)
	size := uint64(obj.objAttr.length)
	assert(size != 0, "no size")
	for i := range obj.objAttr.dimensions {
		size *= obj.objAttr.dimensions[i]
	}
	if nBlocks == 0 {
		logger.Info("No blocks, filling only", size, obj.objAttr.dimensions)
		return makeFillValueReader(obj, nil, int64(size)), size
	}
	segments := make([]*segment, 0)
	firstOffset := uint64(0)
	lastOffset := uint64(size)
	if obj.objAttr.isSlice {
		if len(obj.objAttr.dimensions) > 0 && obj.objAttr.dimensions[0] > 0 {
			dimSize := size / obj.objAttr.dimensions[0]
			firstOffset = uint64(obj.objAttr.firstDim) * dimSize
			lastOffset = uint64(obj.objAttr.lastDim) * dimSize
			size = lastOffset - firstOffset
		}
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
				logger.Warn("cannot seek -- fletcher")
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
				return nil, 0
			}
			bf = newResetReader(zbf, int64(dsLength))
			if firstOffset > 0 {
				logger.Warn("cannot seek -- zlib")
			}
		}
		if shuffleFound {
			logger.Info("using shuffle", dsLength)
			bf = newUnshuffleReader(bf, dsLength, shuffleParam)
			if firstOffset > 0 {
				logger.Info("cannot seek -- shuffle")
			}
		}
		if skipBegin > 0 {
			thisSize := int64(dsLength - (skipBegin + skipEnd))
			if canSeek {
				bf = h5.newSeek(valOffset+skipBegin, thisSize)
			} else {
				var err error
				bf, err = newSkipReader(bf, thisSize, int64(skipBegin), int64(dsLength))
				thrower.ThrowIfError(err)
			}
		}
		thisSeg := &segment{
			offset: offset + skipBegin,
			length: dsLength - (skipBegin + skipEnd),
			r:      bf}
		if int64(thisSeg.offset+thisSeg.length) > h5.fileSize {
			logger.Warn("Offset past file size", hexPrint(thisSeg.offset), thisSeg.length,
				hexPrint(thisSeg.offset+thisSeg.length), hexPrint(uint64(h5.fileSize)))
		}
		segments = append(segments, thisSeg)
		offset += dsLength
	}
	sort.Sort(byOffset{segments})
	readers := make([]io.Reader, 0)
	off := firstOffset
	remOffset := invalidAddress
	logger.Info("firstoffset=", firstOffset, "lastOffset=", lastOffset)
	for i := 0; i < len(segments); i++ {
		r := segments[i].r
		if segments[i].offset > off {
			extra := segments[i].offset - off
			logger.Infof("Fill value reader at offset 0x%x length %d", segments[i].offset-extra,
				extra)
			readers = append(readers, makeFillValueReader(obj, nil, int64(extra)))
			off += extra
		}
		logger.Infof("Reader at offset 0x%x length %d", segments[i].offset, segments[i].length)
		readers = append(readers, newResetReader(r, int64(segments[i].length)))
		off += segments[i].length
		remOffset = segments[i].offset + segments[i].length
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
	return r, size
}

func calcAttrSize(attr *attribute) int64 {
	if len(attr.children) == 0 {
		size := int64(attr.length)
		for _, d := range attr.dimensions {
			size *= int64(d)
		}
		return size
	}
	size := int64(0)
	maxPad := int64(0)
	for _, c := range attr.children {
		pad := int64(0)
		switch c.class {
		case typeFixedPoint, typeFloatingPoint:
			switch c.length {
			case 1: // no padding required
			case 2:
				pad = 1
			case 4:
				pad = 3
			case 8:
				pad = 7
			default:
				fail(fmt.Sprint("attrSize, fixed,float: bad length: ", c.length))
			}
		case typeVariableLength:
			pad = 7
		}
		if pad > maxPad {
			maxPad = pad
		}
		attrSize := calcAttrSize(&c)
		size = (size + pad) & ^pad
		size += attrSize
	}
	size = (size + maxPad) & ^maxPad
	if size < int64(attr.length) {
		// packed
		size = int64(attr.length)
	}
	for i := range attr.dimensions {
		size *= int64(attr.dimensions[i])
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

		switch obj.objAttr.class {
		case typeFixedPoint:
			switch obj.objAttr.length {
			case 1:
				if undefinedFillValue {
					fv := math.MinInt8 + 1
					objFillValue = []byte{byte(fv)}
				}
			case 2:
				if undefinedFillValue {
					fv := int16(math.MinInt16 + 1)
					var bb bytes.Buffer
					err := binary.Write(&bb, obj.objAttr.endian, fv)
					thrower.ThrowIfError(err)
					objFillValue = bb.Bytes()
				}
			case 4:
				if undefinedFillValue {
					fv := int32(math.MinInt32 + 1)
					var bb bytes.Buffer
					err := binary.Write(&bb, obj.objAttr.endian, fv)
					thrower.ThrowIfError(err)
					objFillValue = bb.Bytes()
				}
			case 8:
				if undefinedFillValue {
					fv := int64(math.MinInt64 + 1)
					var bb bytes.Buffer
					err := binary.Write(&bb, obj.objAttr.endian, fv)
					thrower.ThrowIfError(err)
					objFillValue = bb.Bytes()
				}
			}
		// Floating point uses NaN for undefined fill values, not -1
		case typeFloatingPoint:
			switch obj.objAttr.length {
			case 4:
				var fv float32
				if undefinedFillValue {
					fv = float32(math.NaN())
				}
				var buf bytes.Buffer
				err := binary.Write(&buf, obj.objAttr.endian, &fv)
				thrower.ThrowIfError(err)
				objFillValue = buf.Bytes()
				logger.Info("fill value encoded", objFillValue)
			case 8:
				var fv float64
				if undefinedFillValue {
					fv = math.NaN()
				}
				var buf bytes.Buffer
				err := binary.Write(&buf, obj.objAttr.endian, &fv)
				thrower.ThrowIfError(err)
				objFillValue = buf.Bytes()
				logger.Info("fill value encoded", objFillValue)
			default:
				thrower.Throw(ErrInternal)
			}

		// Strings can't have negative lengths or references, so override undefined
		case typeString: // string
			// return all zeros to get zero lengths
			objFillValue = []byte{0}

		// Strings can't have negative lengths or references, so override undefined
		case typeVariableLength:
			objFillValue = []byte{0}

		}
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

// for alignment
func getCountedReader(bf io.Reader, size int64) remReader {
	cbf := bf.(remReader)
	return cbf
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
	bf, _ := h5.newRecordReader(obj, zlibFound, zlibParam, shuffleFound, shuffleParam, fletcher32Found)
	attr := &obj.objAttr
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
	return h5.getDataAttr(bff, *attr)
}

func (h5 *HDF5) getDataAttr(bf io.Reader, attr attribute) interface{} {
	for i, v := range attr.dimensions {
		logger.Info("dimension", i, "=", v)
	}
	var values interface{}
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
	switch attr.class {
	case typeBitField:
		values = allocInt8s(bf, dimensions, false)
		return values

	case typeFixedPoint: // fixed-point
		switch attr.length {
		case 1:
			values = allocInt8s(bf, dimensions, attr.signed)
		case 2:
			values = allocShorts(bf, dimensions, attr.endian, attr.signed)
		case 4:
			values = allocInts(bf, dimensions, attr.endian, attr.signed)
		case 8:
			values = allocInt64s(bf, dimensions, attr.endian, attr.signed)
		default:
			fail(fmt.Sprintf("bad size: %d", attr.length))
		}
		return values // already converted

	case typeFloatingPoint: // floating-point
		switch attr.length {
		case 4:
			values = allocFloats(bf, dimensions, attr.endian)
			logger.Info("done alloc floats, rem=", bf.(remReader).Rem())
		case 8:
			values = allocDoubles(bf, dimensions, attr.endian)
		default:
			fail(fmt.Sprintf("bad size: %d", attr.length))
		}
		return values // already converted

	case typeString: // string
		logger.Info("regular string", len(dimensions))
		return h5.allocRegularStrings(bf, dimensions) // already converted

	case typeVariableLength:
		logger.Info("dimensions=", dimensions)
		if attr.vtType == 1 {
			// It's a string
			// TODO: use the padding and character set information
			logger.Info("variable-length string", len(dimensions))
			return h5.allocStrings(bf, dimensions) // already converted
		}
		logger.Info("variable-length type", typeNames[int(attr.children[0].class)])
		logger.Info("dimensions=", dimensions)
		values = h5.allocVariable(bf, dimensions, attr.children[0])
		logger.Infof("vl kind %T", values)
		return convert(values)

	case typeCompound:
		logger.Info("Alloc compound rem=", bf.(remReader).Rem())
		values = h5.allocCompounds(bf, dimensions, attr)
		return values

	case typeReference:
		return h5.allocReferences(bf, dimensions) // already converted

	case typeEnumerated:
		enumAttr := attr.children[0]
		switch enumAttr.class {
		case typeFixedPoint:
			switch enumAttr.length {
			case 1:
				values = allocInt8s(bf, dimensions, enumAttr.signed)
			case 2:
				values = allocShorts(bf, dimensions, enumAttr.endian, enumAttr.signed)
			case 4:
				values = allocInts(bf, dimensions, enumAttr.endian, enumAttr.signed)
			case 8:
				values = allocInt64s(bf, dimensions, enumAttr.endian, enumAttr.signed)
			default:
				fail(fmt.Sprintf("bad size: %d", enumAttr.length))
			}
		case typeFloatingPoint:
			if floatEnums {
				// Floating point enums are not part of NetCDF.
				switch enumAttr.length {
				case 4:
					values = allocFloats(bf, dimensions, enumAttr.endian)
				case 8:
					values = allocDoubles(bf, dimensions, enumAttr.endian)
				default:
					fail(fmt.Sprintf("bad size: %d", attr.length))
				}
				break
			}
			fallthrough
		default:
			fail(fmt.Sprint("can't handle this class: ", enumAttr.class))
		}
		return enumerated{values}

	case typeArray:
		logger.Info("orig dimensions=", attr.dimensions)
		logger.Info("Array length=", attr.length)
		logger.Info("Array dimensions=", dimensions)
		arrayAttr := attr.children[0]
		logger.Info("child dimensions=", arrayAttr.dimensions)
		logger.Info("childlength=", arrayAttr.length)
		newDimensions := append(dimensions, arrayAttr.dimensions...)
		arrayAttr.dimensions = newDimensions
		logger.Info("new dimensions=", newDimensions)
		cbf := bf.(remReader)
		pad := 0
		switch arrayAttr.class {
		case typeFixedPoint, typeFloatingPoint:
			logger.Info(cbf.Count(), "child length", arrayAttr.length)
			switch arrayAttr.length {
			case 1: // no padding required
			case 2:
				pad = 1
			case 4:
				pad = 3
			case 8:
				pad = 7
			default:
				fail(fmt.Sprint("bad length: ", arrayAttr.length))
			}
		case typeVariableLength:
			pad = 7
		}
		if pad > 0 {
			logger.Info(cbf.Count(), "will pad array")
			if !padBytesCheck(cbf, pad, true /*round*/, logger.Info) {
				logger.Info("2. padbytes problem, file:", h5.fname, "pad=", pad)
			}
		}
		logger.Info(cbf.Count(), "array", "class", arrayAttr.class)
		return h5.getDataAttr(cbf, arrayAttr)

	case typeOpaque:
		return allocOpaque(bf, dimensions, attr.length)

	default:
		logger.Fatal("unhandled type, getDataAttr", attr.class)
	}
	fail("we should have converted everything already")
	panic("silence warning")
}

func (h5 *HDF5) Attributes() api.AttributeMap {
	// entry point, panic can bubble up
	if h5.rootObject == nil {
		nilMap, _ := util.NewOrderedMap(nil, nil)
		return nilMap
	}
	h5.rootObject.sortAttrList()
	return getAttributes(h5.rootObject.attrlist)
}

func hasAddr(obj *object, addr uint64) bool {
	if obj == nil {
		return false
	}
	if addr == obj.addr {
		return true
	}
	if obj.children == nil {
		return false
	}
	for _, o := range obj.children {
		if o.addr == addr {
			return true
		}
		if len(o.children) > 0 {
			if hasAddr(o, addr) {
				return true
			}
		}
	}
	return false
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
		return h5.printType(attrName, attr, origNames)
	}
	return ""
}

func (h5 *HDF5) findVarAttrType(varName string, attrName string) string {
	obj := h5.findVariable(varName)
	if obj == nil {
		return ""
	}
	for _, attr := range obj.attrlist {
		if attr.name != attrName {
			continue
		}
		origNames := map[string]bool{}
		return h5.printType(varName, attr, origNames)
	}
	return ""
}

func (h5 *HDF5) findType(varName string) string {
	obj := h5.findVariable(varName)
	if obj == nil {
		return ""
	}
	origNames := map[string]bool{varName: true}
	return h5.printType(varName, obj.objAttr, origNames)
}

func (h5 *HDF5) getType(typeName string) string {
	obj, has := h5.groupObject.children[typeName]
	if !has {
		return ""
	}
	logger.Info("Found type", typeName, "group", h5.groupName, "child=", obj.name)
	if len(obj.attrlist) != 0 {
		logger.Info("types don't have attributes")
		return ""
	}
	if obj.objAttr.dimensions != nil {
		logger.Info("this is a variable")
		return ""
	}
	origNames := map[string]bool{typeName: true}
	sig := h5.printType(typeName, obj.objAttr, origNames)
	return sig
}

func (h5 *HDF5) getGoType(typeName string) string {
	obj, has := h5.groupObject.children[typeName]
	if !has {
		return ""
	}
	logger.Info("Found type", typeName, "group", h5.groupName, "child=", obj.name)
	if len(obj.attrlist) != 0 {
		logger.Info("types don't have attributes")
		return ""
	}
	if obj.objAttr.dimensions != nil {
		logger.Info("this is a variable")
		return ""
	}
	origNames := map[string]bool{typeName: true}
	sig := h5.printGoType(typeName, obj.objAttr, origNames)
	return fmt.Sprintf("type %s %s", typeName, sig)
}

func (h5 *HDF5) listTypes() []string {
	var ret []string
	for typeName, obj := range h5.groupObject.children {
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

func (h5 *HDF5) findSignature(signature string, name string, origNames map[string]bool,
	printer func(name string, attr attribute, origNames map[string]bool) string) string {
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
		sig := printer(name, obj.objAttr, origNames)
		origNames[varName] = false
		if sig != "" && sig == signature {
			return obj.name
		}
	}
	return ""
}

func (h5 *HDF5) printType(name string, attr attribute, origNames map[string]bool) string {
	switch attr.class {
	case typeFixedPoint:
		prefix := ""
		if !attr.signed {
			prefix = "u"
		}
		switch attr.length {
		case 1:
			return prefix + "byte"
		case 2:
			return prefix + "short"
		case 4:
			return prefix + "int"
		case 8:
			return prefix + "int64"
		}

	case typeFloatingPoint:
		switch attr.length {
		case 4:
			return "float"
		case 8:
			return "double"
		}

	case typeString:
		return "string"

	case typeOpaque:
		signature := fmt.Sprintf("opaque(%d)", attr.length)
		namedType := h5.findSignature(signature, name, origNames, h5.printType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeCompound:
		// TODO: find name of type
		members := make([]string, len(attr.children))
		for i, cattr := range attr.children {
			ty := h5.printType(name, cattr, origNames)
			members[i] = fmt.Sprintf("%s %s", ty, cattr.name)
		}
		interior := strings.Join(members, "; ")
		signature := fmt.Sprintf("compound { %s; };", interior)
		namedType := h5.findSignature(signature, name, origNames, h5.printType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeEnumerated:
		// TODO: find name of type
		assert(len(attr.children) == 1, "enum should have one child")
		enumAttr := attr.children[0]
		assert(len(enumAttr.children) == 0, "no recursion")
		ty := h5.printType(name, enumAttr, origNames)
		assert(ty != "", "unable to parse enum attr")
		list := make([]string, len(enumAttr.enumNames))
		for i, name := range enumAttr.enumNames {
			list[i] = fmt.Sprintf("%s = %v", name, enumAttr.enumValues[i])
		}
		interior := strings.Join(list, ", ")
		signature := fmt.Sprintf("%s enum { %s }", ty, interior)
		namedType := h5.findSignature(signature, name, origNames, h5.printType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeVariableLength:
		// TODO: find name of type
		if attr.vtType == 1 {
			// It's a string
			return "string"
		}
		vAttr := attr.children[0]
		ty := h5.printType(name, vAttr, origNames)
		assert(ty != "", "unable to parse vlen attr")
		signature := fmt.Sprintf("%s(*)", ty)
		namedType := h5.findSignature(signature, name, origNames, h5.printType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeArray:
		arrayAttr := attr.children[0]
		ty := h5.printType(name, arrayAttr, origNames)
		assert(ty != "", "unable to parse array attr")
		dStr := make([]string, len(arrayAttr.dimensions))
		for i, d := range arrayAttr.dimensions {
			dStr[i] = fmt.Sprintf("%d", d)
		}
		dims := strings.Join(dStr, ",")
		signature := fmt.Sprintf("%s(%s)", ty, dims)
		namedType := h5.findSignature(signature, name, origNames, h5.printType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeBitField:
		// Not NetCDF
		return "bitfield"

	case typeReference:
		// Not NetCDF
		return "reference"
	}
	fail(fmt.Sprint("bogus type not handled: ", attr.class, attr.length))
	panic("never gets here")
}

func (h5 *HDF5) printGoType(typeName string, attr attribute, origNames map[string]bool) string {
	switch attr.class {
	case typeFixedPoint:
		prefix := ""
		if !attr.signed {
			prefix = "u"
		}
		switch attr.length {
		case 1:
			return prefix + "int8"
		case 2:
			return prefix + "int16"
		case 4:
			return prefix + "int32"
		case 8:
			return prefix + "int64"
		}

	case typeFloatingPoint:
		switch attr.length {
		case 4:
			return "float32"
		case 8:
			return "float64"
		}

	case typeString:
		return "string"

	case typeOpaque:
		signature := fmt.Sprintf("[%d]uint8", attr.length) // TODO
		namedType := h5.findSignature(signature, typeName, origNames, h5.printGoType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeCompound:
		// TODO: find name of type
		members := make([]string, len(attr.children))
		for i, cattr := range attr.children {
			ty := h5.printGoType(typeName, cattr, origNames)
			members[i] = fmt.Sprintf("\t%s %s", cattr.name, ty)
		}
		interior := strings.Join(members, "\n")
		signature := fmt.Sprintf("struct {\n%s\n}\n", interior)
		namedType := h5.findSignature(signature, typeName, origNames, h5.printGoType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeEnumerated:
		// TODO: find name of type
		assert(len(attr.children) == 1, "enum should have one child")
		enumAttr := attr.children[0]
		assert(len(enumAttr.children) == 0, "no recursion")
		ty := h5.printGoType(typeName, enumAttr, origNames)
		assert(ty != "", "unable to parse enum attr")
		list := make([]string, len(enumAttr.enumNames))
		for i, enumName := range enumAttr.enumNames {
			if i == 0 {
				list[i] = fmt.Sprintf("\t%s %s = %v", enumName, typeName, enumAttr.enumValues[i])
			} else {
				list[i] = fmt.Sprintf("\t%s = %v", enumName, enumAttr.enumValues[i])
			}
		}
		interior := strings.Join(list, "\n")
		signature := fmt.Sprintf("%s\nconst (\n%s\n)\n", ty, interior)
		namedType := h5.findSignature(signature, typeName, origNames, h5.printGoType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeVariableLength:
		if attr.vtType == 1 {
			// It's a string
			return "string"
		}
		vAttr := attr.children[0]
		ty := h5.printGoType(typeName, vAttr, origNames)
		assert(ty != "", "unable to parse vlen attr")
		signature := fmt.Sprintf("[]%s", ty)
		namedType := h5.findSignature(signature, typeName, origNames, h5.printGoType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeArray:
		arrayAttr := attr.children[0]
		ty := h5.printGoType(typeName, arrayAttr, origNames)
		assert(ty != "", "unable to parse array attr")
		dStr := make([]string, len(arrayAttr.dimensions))
		for i, d := range arrayAttr.dimensions {
			dStr[i] = fmt.Sprintf("[%d]", d)
		}
		dims := strings.Join(dStr, "")
		signature := fmt.Sprintf("%s%s", dims, ty)
		namedType := h5.findSignature(signature, typeName, origNames, h5.printGoType)
		if namedType != "" {
			return namedType
		}
		return signature

	case typeBitField:
		// Not NetCDF
		return "bitfield"

	case typeReference:
		// Not NetCDF
		return "reference"
	}
	fail(fmt.Sprint("bogus type not handled: ", attr.class, attr.length))
	panic("never gets here")
}

func getAttributes(unfiltered []attribute) api.AttributeMap {
	filtered := make(map[string]interface{})
	keys := make([]string, 0)
	for _, val := range unfiltered {
		logger.Info("getting attribute", val.name)
		switch val.name {
		case "_Netcdf4Dimid", "_Netcdf4Coordinates", "DIMENSION_LIST", "NAME", "REFERENCE_LIST", "CLASS":
			logger.Infof("Found a %v %v %T", val.name, val.value, val.value)
		default:
			if val.value == nil {
				// TODO: this should only be done if the length is zero, but
				// sometimes we parse non-zero lengths for empty strings.
				filtered[val.name] = ""
				/*
					if val.length == 0 {
						filtered[val.name] = ""
					} else {
						fmt.Printf("%#v\n", val)
						thrower.Throw(ErrInternal)
					}
				*/
			} else {
				// A scalar attribute can be stored as a single-length array
				// This code undoes that.

				fixit := func(value interface{}) interface{} {
					switch v := value.(type) {
					case string:
					case float64:
					case float32:
					case int64:
					case int32:
					case int16:
					case int8:
					case uint64:
					case uint32:
					case uint16:
					case uint8:
						break
					case []enumerated:
						if len(v) == 1 {
							value = v[0]
						}
					case []compound:
						if len(v) == 1 {
							value = v[0]
						}
					case []string:
						if len(v) == 1 {
							value = v[0]
						}
					case []float64:
						if len(v) == 1 {
							value = v[0]
						}
					case []float32:
						if len(v) == 1 {
							value = v[0]
						}
					case []uint64:
						if len(v) == 1 {
							value = v[0]
						}
					case []uint32:
						if len(v) == 1 {
							value = v[0]
						}
					case []uint16:
						if len(v) == 1 {
							value = v[0]
						}
					case []uint8:
						if len(v) == 1 {
							value = v[0]
						}
					case []int64:
						if len(v) == 1 {
							value = v[0]
						}
					case []int32:
						if len(v) == 1 {
							value = v[0]
						}
					case []int16:
						if len(v) == 1 {
							value = v[0]
						}
					case []int8:
						if len(v) == 1 {
							value = v[0]
						}
					default:
						logger.Infof("Strange attribute type %T", value)
					}
					return value
				}
				value := fixit(val.value)
				e, has := value.(enumerated)
				if has {
					e.values = fixit(e.values)
					value = e
				}
				filtered[val.name] = value
			}
			keys = append(keys, val.name)
		}
	}
	om, err := util.NewOrderedMap(keys, filtered)
	thrower.ThrowIfError(err)
	om.Hide(ncpKey)
	return om
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
	for _, a := range obj.attrlist {
		if a.name != "DIMENSION_LIST" {
			continue
		}
		logger.Infof("DIMENSION_LIST=%T 0x%x", a.value, a.value)
		varLen := a.value.([][]int64)
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
		for _, a := range ob.attrlist {
			if a.name != "REFERENCE_LIST" {
				continue
			}
			logger.Infof("value is %T %v", a.value, a.value)
			for k, v := range a.value.([]compound) {
				vals2 := v
				v0 := vals2[0].(int64)
				v1 := vals2[1].(int32)
				logger.Infof("single ref %d 0x%x %d %s", k, v0, v1, ob.name)
			}
		}
		for _, o := range ob.children {
			f(o)
		}
	}
	f(h5.rootObject)
	for _, a := range obj.attrlist {
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

func (h5 *HDF5) GetVariable(varName string) (av *api.Variable, err error) {
	err = ErrInternal
	defer thrower.RecoverError(&err)
	logger.Info("get variable", varName)
	found := h5.findVariable(varName)
	if found == nil {
		logger.Infof("variable %s not found", varName)
		return nil, ErrNotFound
	}
	data := h5.getData(found)
	if data == nil {
		return nil, ErrNotFound
	}
	found.sortAttrList()
	dims := h5.getDimensions(found)
	attrs := getAttributes(found.attrlist)
	return &api.Variable{
			Values:     data,
			Dimensions: dims,
			Attributes: attrs},
		nil
}

func (h5 *HDF5) GetVarGetter(varName string) (slicer api.VarGetter, err error) {
	defer thrower.RecoverError(&err)
	found := h5.findVariable(varName)
	if found == nil {
		logger.Warnf("variable %s not found", varName)
		return nil, ErrNotFound
	}
	found.sortAttrList()
	getSlice := func(begin, end int64) (interface{}, error) {
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
	attrs := getAttributes(found.attrlist)
	return internal.NewSlicer(getSlice, int64(found.objAttr.dimensions[0]),
		dims,
		attrs), nil
}

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

func (obj *object) sortAttrList() {
	if obj.attrListIsSorted {
		return
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
						logger.Info(o.name, "skip because is a dimension")
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
