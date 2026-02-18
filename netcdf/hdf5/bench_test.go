package hdf5

// Benchmarks for the three performance improvements:
//
//  1. BenchmarkVlenStrings  – global heap caching in readGlobalHeap.
//     Each vlen string element requires a readGlobalHeap call.  Without the
//     cache, reading element k requires scanning past k-1 earlier entries
//     (O(N²) total).  With the cache the heap is parsed once and every
//     subsequent access is an O(1) map lookup.
//
//  2. BenchmarkManyVariablesDimensions – addrIndex replaces findDim.
//     getDimensions was calling findDim (O(N) tree traversal) for every
//     dimension of every variable.  addrIndex() builds a flat address→name
//     map once and all lookups are O(1) after that.
//
//  3. BenchmarkListVariables – classifyObject + dead-loop removal.
//     ListVariables used to run a redundant inline attribute-classification
//     loop for each child object before calling findVariable (which did the
//     same scan again).  Now it calls findVariable directly.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ncgenBench compiles a CDL string to a temporary .nc file via ncgen.
// The file is cleaned up automatically when b's TempDir is removed.
func ncgenBench(b *testing.B, cdl string) string {
	b.Helper()
	dir := b.TempDir()
	cdlPath := filepath.Join(dir, "bench.cdl")
	ncPath := filepath.Join(dir, "bench.nc")
	if err := os.WriteFile(cdlPath, []byte(cdl), 0o644); err != nil {
		b.Fatalf("write cdl: %v", err)
	}
	out, err := exec.Command("ncgen", "-k", "nc4", "-o", ncPath, cdlPath).CombinedOutput()
	if err != nil {
		b.Fatalf("ncgen: %v\n%s", err, out)
	}
	return ncPath
}

// vlenStringsCDL generates a NetCDF4 CDL with a single vlen-string variable
// of length n.  Each element is stored as a separate object in the global heap,
// so reading all n elements requires n readGlobalHeap calls.
func vlenStringsCDL(n int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "netcdf benchvlen {\ndimensions:\n  n = %d;\nvariables:\n  string sv(n);\ndata:\n  sv = ", n)
	for i := range n {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "\"str%06d\"", i)
	}
	sb.WriteString(";\n}\n")
	return sb.String()
}

// manyVarsCDL generates a NetCDF4 CDL with nVars float variables, each with
// two unique dimensions from a pool of nVars*2 dimensions.  getDimensions is
// called once per GetVariable and must resolve each dimension to a name.
func manyVarsCDL(nVars int) string {
	var sb strings.Builder
	sb.WriteString("netcdf benchvars {\ndimensions:\n")
	for i := range nVars * 2 {
		fmt.Fprintf(&sb, "  d%d = 4;\n", i)
	}
	sb.WriteString("variables:\n")
	for i := range nVars {
		fmt.Fprintf(&sb, "  float v%d(d%d, d%d);\n", i, i*2, i*2+1)
	}
	sb.WriteString("data:\n")
	for i := range nVars {
		fmt.Fprintf(&sb, "  v%d = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16;\n", i)
	}
	sb.WriteString("}\n")
	return sb.String()
}

// BenchmarkVlenStrings measures the cost of reading all elements of a large
// vlen-string variable.  The global heap cache turns the per-Open cost from
// O(N²) heap scans to O(N).
func BenchmarkVlenStrings(b *testing.B) {
	for _, n := range []int{100, 500, 1000} {
		n := n
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ncPath := ncgenBench(b, vlenStringsCDL(n))
			b.ResetTimer()
			for b.Loop() {
				nc, err := Open(ncPath)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := nc.GetVariable("sv"); err != nil {
					b.Fatal(err)
				}
				nc.Close()
			}
		})
	}
}

// BenchmarkManyVariablesDimensions measures the cost of calling GetVariable
// on every variable in a file with many distinct dimensions.  addrIndex turns
// each getDimensions call from O(N) tree traversal to O(1) map lookup (after
// the map is built on the first call).
func BenchmarkManyVariablesDimensions(b *testing.B) {
	for _, nVars := range []int{25, 50, 100} {
		nVars := nVars
		b.Run(fmt.Sprintf("vars=%d", nVars), func(b *testing.B) {
			ncPath := ncgenBench(b, manyVarsCDL(nVars))
			// Pre-collect variable names outside the timed loop.
			nc0, err := Open(ncPath)
			if err != nil {
				b.Fatal(err)
			}
			varNames := nc0.ListVariables()
			nc0.Close()

			b.ResetTimer()
			for b.Loop() {
				nc, err := Open(ncPath)
				if err != nil {
					b.Fatal(err)
				}
				for _, name := range varNames {
					if _, err := nc.GetVariable(name); err != nil {
						b.Fatal(err)
					}
				}
				nc.Close()
			}
		})
	}
}

// BenchmarkListVariables measures the cost of listing variables and dimensions
// in a group with many children.  classifyObject + elimination of the dead
// inner classification loop in ListVariables removes redundant attribute scans.
func BenchmarkListVariables(b *testing.B) {
	for _, nVars := range []int{25, 50, 100} {
		nVars := nVars
		b.Run(fmt.Sprintf("vars=%d", nVars), func(b *testing.B) {
			ncPath := ncgenBench(b, manyVarsCDL(nVars))
			b.ResetTimer()
			for b.Loop() {
				nc, err := Open(ncPath)
				if err != nil {
					b.Fatal(err)
				}
				_ = nc.ListVariables()
				_ = nc.ListDimensions()
				_ = nc.ListTypes()
				nc.Close()
			}
		})
	}
}
