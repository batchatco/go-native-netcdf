package cdf

// 1. Need test of invalid fill value (slice instead of scalar)
// 2. Need test of V1 file.
// 3. Need test of null in name.
// 4. Need test of too many dimensions.

import (
	"testing"
)

func common(b testing.TB, num int, slow convertType) {
	b.Helper()
	fileName := "testdata/solarforcing_small.nc"
	for range num {
		nc, err := Open(fileName)
		nc.(*CDF).slowConvert = slow
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

func BenchmarkFast(b *testing.B) {
	common(b, 1000, fast)
}

func BenchmarkSlow(b *testing.B) {
	common(b, 1000, slow)
}

func TestPerf(t *testing.T) {
	common(t, 1, fast)
}

func TestSlow(t *testing.T) {
	common(t, 1, slow)
}
