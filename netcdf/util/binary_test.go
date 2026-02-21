package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/batchatco/go-thrower"
)

// errWriter is an io.Writer and io.ByteWriter that always returns an error.
type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }
func (e errWriter) WriteByte(c byte) error      { return e.err }

// errReader is an io.Reader that always returns an error.
type errReader struct{ err error }

func (e errReader) Read(p []byte) (int, error) { return 0, e.err }

var errIO = errors.New("io error")

func TestMustWriteLE(t *testing.T) {
	var buf bytes.Buffer
	MustWriteLE(&buf, uint32(0xDEADBEEF))
	var got uint32
	if err := binary.Read(&buf, binary.LittleEndian, &got); err != nil {
		t.Fatal(err)
	}
	if got != 0xDEADBEEF {
		t.Errorf("got 0x%X, want 0xDEADBEEF", got)
	}
}

func TestMustWriteBE(t *testing.T) {
	var buf bytes.Buffer
	MustWriteBE(&buf, uint32(0xDEADBEEF))
	var got uint32
	if err := binary.Read(&buf, binary.BigEndian, &got); err != nil {
		t.Fatal(err)
	}
	if got != 0xDEADBEEF {
		t.Errorf("got 0x%X, want 0xDEADBEEF", got)
	}
}

func TestMustWrite(t *testing.T) {
	var buf bytes.Buffer
	MustWrite(&buf, binary.LittleEndian, uint16(0x1234))
	var got uint16
	if err := binary.Read(&buf, binary.LittleEndian, &got); err != nil {
		t.Fatal(err)
	}
	if got != 0x1234 {
		t.Errorf("got 0x%X, want 0x1234", got)
	}
}

func TestMustWriteByte(t *testing.T) {
	var buf bytes.Buffer
	MustWriteByte(&buf, 0xAB)
	b := buf.Bytes()
	if len(b) != 1 || b[0] != 0xAB {
		t.Errorf("got %v, want [0xAB]", b)
	}
}

func TestMustWriteRaw(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte{1, 2, 3, 4}
	MustWriteRaw(&buf, payload)
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Errorf("got %v, want %v", buf.Bytes(), payload)
	}
}

func TestMustReadLE(t *testing.T) {
	var buf bytes.Buffer
	MustWriteLE(&buf, uint32(0xCAFEBABE))
	var got uint32
	MustReadLE(&buf, &got)
	if got != 0xCAFEBABE {
		t.Errorf("got 0x%X, want 0xCAFEBABE", got)
	}
}

func TestMustReadBE(t *testing.T) {
	var buf bytes.Buffer
	MustWriteBE(&buf, uint32(0xCAFEBABE))
	var got uint32
	MustReadBE(&buf, &got)
	if got != 0xCAFEBABE {
		t.Errorf("got 0x%X, want 0xCAFEBABE", got)
	}
}

func TestMustRead(t *testing.T) {
	var buf bytes.Buffer
	MustWrite(&buf, binary.LittleEndian, int16(-42))
	var got int16
	MustRead(&buf, binary.LittleEndian, &got)
	if got != -42 {
		t.Errorf("got %d, want -42", got)
	}
}

func TestMustRead8(t *testing.T) {
	r := bytes.NewReader([]byte{0x7F})
	got := MustRead8(r)
	if got != 0x7F {
		t.Errorf("got 0x%X, want 0x7F", got)
	}
}

func TestMustWriteError(t *testing.T) {
	err := func() (e error) {
		defer thrower.RecoverError(&e)
		MustWrite(errWriter{errIO}, binary.LittleEndian, uint32(0))
		return nil
	}()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMustWriteByteError(t *testing.T) {
	err := func() (e error) {
		defer thrower.RecoverError(&e)
		MustWriteByte(errWriter{errIO}, 0)
		return nil
	}()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMustWriteRawError(t *testing.T) {
	err := func() (e error) {
		defer thrower.RecoverError(&e)
		MustWriteRaw(errWriter{errIO}, []byte{1})
		return nil
	}()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMustReadError(t *testing.T) {
	err := func() (e error) {
		defer thrower.RecoverError(&e)
		var got uint32
		MustRead(errReader{errIO}, binary.LittleEndian, &got)
		return nil
	}()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMustRead8Error(t *testing.T) {
	err := func() (e error) {
		defer thrower.RecoverError(&e)
		MustRead8(bytes.NewReader(nil))
		return nil
	}()
	if !errors.Is(err, io.EOF) {
		t.Errorf("got %v, want io.EOF", err)
	}
}
