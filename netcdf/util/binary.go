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
