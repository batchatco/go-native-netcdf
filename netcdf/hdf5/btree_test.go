package hdf5

// TestBTreeInternal exercises readBTreeInternal by opening a file whose root
// group contains enough objects that the HDF5 library must use a B-tree with
// internal (non-leaf) nodes (depth > 0).  With 100 float variables each
// having two unique dimensions, the root group holds 300 objects, well above
// the single-leaf threshold (~45 for HDF5's default 512-byte node size with
// 11-byte records).

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func ncgenTest(t *testing.T, cdl string) string {
	t.Helper()
	if _, err := exec.LookPath(ncGenBinary); err != nil {
		t.Skip("ncgen not in PATH:", err)
	}
	dir := t.TempDir()
	cdlPath := filepath.Join(dir, "test.cdl")
	ncPath := filepath.Join(dir, "test.nc")
	if err := os.WriteFile(cdlPath, []byte(cdl), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(ncGenBinary, "-k", "nc4", "-o", ncPath, cdlPath).CombinedOutput()
	if err != nil {
		t.Fatalf("ncgen: %v\n%s", err, out)
	}
	return ncPath
}

// largeManyVarsCDL generates CDL with nVars float variables, each with two
// unique dimensions.  The root group will have nVars + 2*nVars = 3*nVars
// named objects, which is enough to trigger B-tree internal nodes.
func largeManyVarsCDL(nVars int) string {
	var sb strings.Builder
	sb.WriteString("netcdf btreetest {\ndimensions:\n")
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

func TestBTreeInternal(t *testing.T) {
	const nVars = 100 // 300 total objects → B-tree depth > 0
	ncPath := ncgenTest(t, largeManyVarsCDL(nVars))

	nc, err := Open(ncPath)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	vars := nc.ListVariables()
	if len(vars) != nVars {
		t.Errorf("got %d variables, want %d", len(vars), nVars)
	}
	for _, name := range vars {
		v, err := nc.GetVariable(name)
		if err != nil {
			t.Errorf("GetVariable(%q): %v", name, err)
			continue
		}
		f32s, ok := v.Values.([][]float32)
		if !ok {
			t.Errorf("variable %q: want [][]float32, got %T", name, v.Values)
		} else if len(f32s) != 4 || len(f32s[0]) != 4 {
			t.Errorf("variable %q: want 4×4, got %d×%d", name, len(f32s), len(f32s[0]))
		}
	}
}
