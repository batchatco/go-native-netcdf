package hdf5

// 1. Need test of invalid fill value (slice instead of scalar)
// 2. Need test of V1 file.
// 3. Need test of null in name.
// 4. Need test of too many dimensions.

import (
	"testing"
)

func common(b testing.TB, num int) {
	b.Helper()
	fileName := "testdata/oco2.nc4"
	for i := 0; i < num; i++ {
		nc, err := Open(fileName)
		if err != nil {
			b.Error(err)
			return
		}
		for _, n := range nc.ListVariables() {
			v, err := nc.GetVariable(n)
			if err != nil {
				b.Error("var", n, "missing", err)
				return
			}
			_ = v.Dimensions
			for _, k := range v.Attributes.Keys() {
				_, has := v.Attributes.Get(k)
				if !has {
					b.Error("Missing attr", k)
					return
				}
			}
			_ = v.Values
		}
		for _, n := range nc.Attributes().Keys() {
			_, has := nc.Attributes().Get(n)
			if !has {
				b.Error("Missing global attr", n)
				return
			}
		}
		nc.Close()
	}
}

func BenchmarkHDF5(b *testing.B) {
	common(b, b.N)
}

func TestPerf(b *testing.T) {
	common(b, 1)
}
