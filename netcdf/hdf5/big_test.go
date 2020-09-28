package hdf5

import (
	"os"
	"reflect"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/api"
	"github.com/batchatco/go-native-netcdf/netcdf/cdf"
)

func (kl keyValList) checkSlicer(t testing.TB, name string, slicer api.VarGetter) bool {
	t.Helper()
	for _, kv := range kl {
		if name != kv.name {
			continue
		}
		t.Log(name, "slicer len=", slicer.Len())
		for i := int64(0); i < slicer.Len(); i++ {
			slice, err := slicer.GetSlice(i, i+1)
			if err != nil {
				t.Error(err)
				return false
			}
			_ = slice
		}
		t.Log("Attributes")
		if !reflect.DeepEqual(slicer.Attributes().Keys(), kv.val.Attributes.Keys()) &&
			!(len(slicer.Attributes().Keys()) == 0 && len(kv.val.Attributes.Keys()) == 0) {
			t.Log(name, "attr")
			return false
		}
		for _, v := range slicer.Attributes().Keys() {
			a, has := slicer.Attributes().Get(v)
			if !has {
				t.Log(name, "get val")
				return false
			}
			b, has := kv.val.Attributes.Get(v)
			if !has {
				t.Log(name, "get kv")
				return false
			}
			rb := reflect.ValueOf(b)
			if rb.Kind() == reflect.Slice && rb.Len() == 1 {
				// Special case: single length arrays are returned as scalars
				elem := rb.Index(0)
				b = elem.Interface()
			}
			if !reflect.DeepEqual(a, b) {
				t.Logf(name, "attr deepequal %s %T %T", name, a, b)
				return false
			}
		}
		if !reflect.DeepEqual(slicer.Dimensions(), kv.val.Dimensions) &&
			!(len(slicer.Dimensions()) == 0 && len(kv.val.Dimensions) == 0) {
			t.Log(name, "dims", slicer.Dimensions(), kv.val.Dimensions)
			return false
		}
		return true
	}
	return false
}

func checkAllSlicer(t testing.TB, nc api.Group, values keyValList) {
	t.Helper()
	vars := nc.ListVariables()
	if len(vars) != len(values) {
		t.Error("mismatch", "got=", len(vars), "exp=", len(values))
		return
	}
	for _, name := range vars {
		slicer, err := nc.GetVarGetter(name)
		if err != nil {
			t.Error(err)
			continue
		}
		if !values.checkSlicer(t, name, slicer) {
			t.Error("check")
			continue
		}
	}
}

type wrapper struct {
	vg        api.VarGetter
	chunkSize int
	slice     [][]int16
	begin     int
	end       int
}

func newWrapper(nc api.Group, varName string, chunkSize int) (*wrapper, error) {
	vg, err := nc.GetVarGetter(varName)
	if err != nil {
		return nil, err
	}
	return &wrapper{vg, chunkSize, nil, 0, 0}, nil
}

func (w *wrapper) Get(ilat int, ilon int) int16 {
	if w.slice == nil || !(ilat >= w.begin && ilat < w.end) {
		round := w.chunkSize * (ilat / w.chunkSize)
		var err error
		var sl interface{}
		sl, err = w.vg.GetSlice(int64(round), int64(round+w.chunkSize))
		if err != nil {
			panic(err)
		}
		w.slice = sl.([][]int16)
		w.begin = round
		w.end = round + w.chunkSize
	}
	return w.slice[ilat-int(w.begin)][ilon]
}

func TestBigSlice(t *testing.T) {
	enabled := os.Getenv("BIG_TESTS")
	if enabled != "true" {
		t.Log("Test disabled by default. Enabled by setting BIG_TESTS env var.")
		return
	}
	// Source:
	// https://www.bodc.ac.uk/data/open_download/gebco/gebco_2020/zip/
	fileName := "testdata/GEBCO_2020.nc"
	nc, err := Open(fileName)
	if err != nil {
		t.Error(err)
		return
	}
	// nccopy -k cdf5 GEBCO_2020.nc" GEBCO_2020cdf.nc"
	cdfFileName := "testdata/GEBCO_2020cdf.nc"
	cdf, err := cdf.Open(cdfFileName)
	if err != nil {
		t.Error(err)
		return
	}
	const chunkSize = 2400
	w, err := newWrapper(nc, "elevation", chunkSize)
	if err != nil {
		t.Error(err)
		return
	}
	cw, err := newWrapper(cdf, "elevation", chunkSize)
	if err != nil {
		t.Error(err)
		return
	}
	for lat := 0; lat < 43200; lat += chunkSize {
		for lon := 0; lon < 86400; lon++ {
			elev := w.Get(lat, lon)
			cdfElev := cw.Get(lat, lon)
			if elev != cdfElev {
				t.Error("mismatch", lat, lon, elev, cdfElev)
				return
			}
		}
	}
}

func BenchmarkTestBig(b *testing.B) {
	fileName := "testdata/GEBCO_2020.nc"
	nc, err := Open(fileName)
	if err != nil {
		b.Error(err)
		return
	}
	exp := keyValList{
		{"lat", "double", api.Variable{
			Values:     []float64{5},
			Attributes: nilMap,
			Dimensions: []string{"lat"},
		}},
		{"lon", "double", api.Variable{
			Values:     []float64{7},
			Attributes: nilMap,
			Dimensions: []string{"lon"},
		}},
		{"elevation", "short", api.Variable{
			Values: [][]int8{
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
			},
			Attributes: nilMap,
			Dimensions: []string{"lat", "lon"},
		}},
	}
	defer nc.Close()
	checkAllSlicer(b, nc, exp)
}
