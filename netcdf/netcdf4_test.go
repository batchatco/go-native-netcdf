package netcdf

import (
	"os"
	"testing"
)

var filenames = []string{"empty", "bogus", "cdf.nc", "hdf5.nc"}

var errs = []error{ErrUnknown, ErrUnknown, nil, nil}

func TestNew(t *testing.T) {
	for i, name := range filenames {
		f, err := os.Open("testdata/" + name)
		if err != nil {
			t.Error("os.Open", name, err)
		} else {
			g, err := New(f)
			if err != errs[i] {
				t.Error("New", name, "expected", errs[i], "got", err)
			}
			if g != nil {
				g.Close()
			} else {
				f.Close()
			}
		}
	}
}

func TestOpen(t *testing.T) {
	_, err := Open("testdata/bogus")
	if err != ErrUnknown {
		t.Error("expected error")
	}
	for i, name := range filenames {
		g, err := Open("testdata/" + name)
		if err != errs[i] {
			t.Error("Open", name, "expected", errs[i], "got", err)
		}
		if g != nil {
			g.Close()
		}
	}
}
