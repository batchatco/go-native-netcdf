package util

import (
	"encoding/binary"
	"io"

	"github.com/batchatco/go-thrower"
)

// MustWrite wraps binary.Write and throws an error if it fails.
func MustWrite(w io.Writer, order binary.ByteOrder, data any) {
	err := binary.Write(w, order, data)
	thrower.ThrowIfError(err)
}

// MustWriteLE wraps binary.Write with LittleEndian and throws an error if it fails.
func MustWriteLE(w io.Writer, data any) {
	MustWrite(w, binary.LittleEndian, data)
}

// MustWriteBE wraps binary.Write with BigEndian and throws an error if it fails.
func MustWriteBE(w io.Writer, data any) {
	MustWrite(w, binary.BigEndian, data)
}

// MustRead wraps binary.Read and throws an error if it fails.
func MustRead(r io.Reader, order binary.ByteOrder, data any) {
	err := binary.Read(r, order, data)
	thrower.ThrowIfError(err)
}

// MustReadLE wraps binary.Read with LittleEndian and throws an error if it fails.
func MustReadLE(r io.Reader, data any) {
	MustRead(r, binary.LittleEndian, data)
}

// MustReadBE wraps binary.Read with BigEndian and throws an error if it fails.
func MustReadBE(r io.Reader, data any) {
	MustRead(r, binary.BigEndian, data)
}

// MustRead8 reads a single byte and throws an error if it fails.
func MustRead8(r io.Reader) byte {
	var b byte
	MustReadLE(r, &b)
	return b
}
