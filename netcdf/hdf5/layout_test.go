package hdf5

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestLayout(t *testing.T) {
	fileNameNoExt := "testlayout"
	genName := ncGen(t, fileNameNoExt)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	chunkedName := "testdata/" + fileNameNoExt + "_chunk.nc"
	cmdString := []string{"-l", "a:CHUNK=7x6", genName, chunkedName}
	cmd := exec.Command(h5RepackBinary, cmdString...)
	err := cmd.Run()
	if err != nil {
		s := strings.Join(cmdString, " ")
		t.Error(h5RepackBinary, s, ":", err)
		return
	}
	defer os.Remove(chunkedName)

	nc, err := Open(genName)
	if err != nil {
		t.Error("open", genName)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("a")
	if err != nil {
		t.Error("get gen var -- not found")
		return
	}
	vals, ok := vr.Values.([][]int32)
	if !ok {
		t.Error("get gen var -- wrong type")
		return
	}
	gnc, err := Open(chunkedName)
	if err != nil {
		t.Error("open", chunkedName)
		return
	}
	defer gnc.Close()
	gvr, err := gnc.GetVariable("a")
	if err != nil {
		t.Error("get chunked var -- not found")
		return
	}
	gvals, ok := gvr.Values.([][]int32)
	if !ok {
		t.Error("get chunked var -- wrong type")
		return
	}
	for i := range len(vals) {
		for j := range len(vals[i]) {
			if vals[i][j] != gvals[i][j] {
				t.Error("value mismatch at", i, j, vals[i][j], gvals[i][j])
				return
			}
		}
	}
}

func TestLayout2(t *testing.T) {
	fileNameNoExt := "testlayout2"
	genName := ncGen(t, fileNameNoExt)
	if genName == "" {
		t.Error(errorNcGen)
		return
	}
	defer os.Remove(genName)
	chunkedName := "testdata/" + fileNameNoExt + "_chunk.nc"
	cmdString := []string{"-l", "a:CHUNK=1x7x6", genName, chunkedName}
	cmd := exec.Command(h5RepackBinary, cmdString...)
	err := cmd.Run()
	if err != nil {
		s := strings.Join(cmdString, " ")
		t.Error(h5RepackBinary, s, ":", err)
		return
	}
	defer os.Remove(chunkedName)

	nc, err := Open(genName)
	if err != nil {
		t.Error("open", genName)
		return
	}
	defer nc.Close()
	vr, err := nc.GetVariable("a")
	if err != nil {
		t.Error("get gen var -- not found")
		return
	}
	vals, ok := vr.Values.([][][]int32)
	if !ok {
		t.Error("get gen var -- wrong type")
		return
	}
	gnc, err := Open(chunkedName)
	if err != nil {
		t.Error("open", chunkedName)
		return
	}
	defer gnc.Close()
	gvr, err := gnc.GetVariable("a")
	if err != nil {
		t.Error("get chunked var -- not found")
		return
	}
	gvals, ok := gvr.Values.([][][]int32)
	if !ok {
		t.Error("get chunked var -- wrong type")
		return
	}
	for i := range vals {
		for j := range vals[i] {
			for k := range vals[i][j] {
				if vals[i][j][k] != gvals[i][j][k] {
					t.Error("value mismatch at", i, j, k, ":", "got", gvals[i][j][k], "want", vals[i][j][k])
				}
			}
		}
	}
}
