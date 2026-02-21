package netcdf

import (
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

// writeAndRead writes one float32 variable via OpenWriter of the given kind,
// closes the writer, re-opens the file with Open, and returns the read values.
func writeAndRead(t *testing.T, kind FileKind) (any, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "netcdf_test_*.nc")
	if err != nil {
		t.Fatal(err)
	}
	fname := f.Name()
	f.Close()

	w, err := OpenWriter(fname, kind)
	if err != nil {
		return nil, err
	}

	attrs, _ := util.NewOrderedMap(nil, nil)
	err = w.AddVar("x", api.Variable{
		Values:     []float32{1.0, 2.0, 3.0},
		Dimensions: []string{"n"},
		Attributes: attrs,
	})
	if err != nil {
		_ = w.Close()
		t.Fatalf("AddVar: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g, err := Open(fname)
	if err != nil {
		return nil, err
	}
	defer g.Close()

	vr, err := g.GetVariable("x")
	if err != nil {
		return nil, err
	}
	return vr.Values, nil
}

func TestOpenWriterCDF(t *testing.T) {
	got, err := writeAndRead(t, KindCDF)
	if err != nil {
		t.Fatalf("KindCDF round-trip: %v", err)
	}
	want := []float32{1.0, 2.0, 3.0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("KindCDF: got %v, want %v", got, want)
	}
}

func TestOpenWriterHDF5(t *testing.T) {
	got, err := writeAndRead(t, KindHDF5)
	if err != nil {
		t.Fatalf("KindHDF5 round-trip: %v", err)
	}
	want := []float32{1.0, 2.0, 3.0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("KindHDF5: got %v, want %v", got, want)
	}
}

func TestOpenWriterUnknownKind(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "netcdf_test_*.nc")
	if err != nil {
		t.Fatal(err)
	}
	fname := f.Name()
	f.Close()

	_, err = OpenWriter(fname, FileKind(999))
	if !errors.Is(err, ErrUnknown) {
		t.Errorf("got %v, want ErrUnknown", err)
	}
}

func TestOpenWriterBadPath(t *testing.T) {
	_, err := OpenWriter("/nonexistent-dir/test.nc", KindCDF)
	if err == nil {
		t.Fatal("expected error for bad path, got nil")
	}
	if errors.Is(err, ErrUnknown) {
		t.Errorf("expected OS error, got ErrUnknown")
	}

	_, err = OpenWriter("/nonexistent-dir/test.nc", KindHDF5)
	if err == nil {
		t.Fatal("expected error for bad path, got nil")
	}
	if errors.Is(err, ErrUnknown) {
		t.Errorf("expected OS error, got ErrUnknown")
	}
}
