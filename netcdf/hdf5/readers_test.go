package hdf5

import (
	"bytes"
	"testing"

	"github.com/batchatco/go-thrower"
)

func TestFletcherOdd(t *testing.T) {
	const nbytes = 11
	// Create the input reader
	b := make([]byte, nbytes+4)
	// Put in the checksum
	copy(b[nbytes:], []byte{0x19, 0x1e, 0x50, 0x46})
	// Then write the data
	for i := 0; i < nbytes; i++ {
		b[i] = byte(i)
	}
	r := bytes.NewReader(b)

	// create the Fletcher32 reader and verify we can read from it
	fl := newFletcher32Reader(r, uint64(len(b)))
	var b2 [nbytes]byte
	n, err := fl.Read(b2[:])
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(b2) {
		t.Error("Got", n, "expected", len(b2))
		return
	}
	for i := 0; i < nbytes; i++ {
		if b2[i] != b[i] {
			t.Error("Got", b2[i], "at offset", i, "expected", b[i])
		}
	}

	// try again with the old reader
	r = bytes.NewReader(b)
	fl = oldFletcher32Reader(r, uint64(len(b)))
	n, err = fl.Read(b2[:])
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(b2) {
		t.Error("Got", n, "expected", len(b2))
		return
	}
	for i := 0; i < nbytes; i++ {
		if b2[i] != b[i] {
			t.Error("Got", b2[i], "at offset", i, "expected", b[i])
		}
	}
}

func TestFletcherEven(t *testing.T) {
	const nbytes = 10
	// Create the input reader
	b := make([]byte, nbytes+4)
	// Put in the checksum
	copy(b[nbytes:], []byte{0x19, 0x14, 0x37, 0x28})
	// Then write the data
	for i := 0; i < nbytes; i++ {
		b[i] = byte(i)
	}
	r := bytes.NewReader(b)

	// create the Fletcher32 reader and verify we can read from it
	fl := newFletcher32Reader(r, uint64(len(b)))
	var b2 [nbytes]byte
	n, err := fl.Read(b2[:])
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(b2) {
		t.Error("Got", n, "expected", len(b2))
		return
	}
	for i := 0; i < nbytes; i++ {
		if b2[i] != b[i] {
			t.Error("Got", b2[i], "at offset", i, "expected", b[i])
		}
	}

	// try again with the old reader
	r = bytes.NewReader(b)
	fl = oldFletcher32Reader(r, uint64(len(b)))
	n, err = fl.Read(b2[:])
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(b2) {
		t.Error("Got", n, "expected", len(b2))
		return
	}
	for i := 0; i < nbytes; i++ {
		if b2[i] != b[i] {
			t.Error("Got", b2[i], "at offset", i, "expected", b[i])
		}
	}
}

// Test that bad checksums properly fail
func TestFletcherFail(t *testing.T) {
	const nbytes = 10
	// Create the input reader
	b := make([]byte, nbytes+4)
	// Put in the checksum
	copy(b[nbytes:], []byte{0xde, 0xad, 0xbe, 0xef})
	// Then write the data
	for i := 0; i < nbytes; i++ {
		b[i] = byte(i)
	}

	err := func() (err error) {
		defer thrower.RecoverError(&err)
		r := bytes.NewReader(b)
		fl := newFletcher32Reader(r, uint64(len(b)))
		// create the Fletcher32 reader and verify we can read from it
		var b2 [nbytes]byte
		_, err = fl.Read(b2[:])
		if err != nil {
			t.Error(err)
			thrower.Throw(err)
		}
		return nil
	}()
	if err != ErrFletcherChecksum {
		t.Error("Got", err, "expected", ErrFletcherChecksum)
		return
	}

	// try again with the old reader
	err = func() (err error) {
		defer thrower.RecoverError(&err)
		r := bytes.NewReader(b)
		fl := oldFletcher32Reader(r, uint64(len(b)))
		var b2 [nbytes]byte
		_, err = fl.Read(b2[:])
		if err != nil {
			t.Error(err)
			thrower.Throw(err)
		}
		return nil
	}()
	if err != ErrFletcherChecksum {
		t.Error("Got", err, "expected", ErrFletcherChecksum)
		return
	}
}

func TestFletcherSingle(t *testing.T) {
	const nbytes = 1
	// Create the input reader
	b := make([]byte, nbytes+4)
	// Put in the checksum
	copy(b[nbytes:], []byte{0x00, 0x01, 0x00, 0x01})
	// Then write the data
	for i := 0; i < nbytes; i++ {
		b[i] = byte(i) + 1
	}
	r := bytes.NewReader(b)

	// create the Fletcher32 reader and verify we can read from it
	fl := newFletcher32Reader(r, uint64(len(b)))
	var b2 [nbytes]byte
	n, err := fl.Read(b2[:])
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(b2) {
		t.Error("Got", n, "expected", len(b2))
		return
	}
	for i := 0; i < nbytes; i++ {
		if b2[i] != b[i] {
			t.Error("Got", b2[i], "at offset", i, "expected", b[i])
		}
	}

	// try again with the old reader
	r = bytes.NewReader(b)
	fl = oldFletcher32Reader(r, uint64(len(b)))
	n, err = fl.Read(b2[:])
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(b2) {
		t.Error("Got", n, "expected", len(b2))
		return
	}
	for i := 0; i < nbytes; i++ {
		if b2[i] != b[i] {
			t.Error("Got", b2[i], "at offset", i, "expected", b[i])
		}
	}
}
