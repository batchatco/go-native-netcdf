package hdf5

import (
	"fmt"
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
	t.Log("[row,col] src,dst\n")
	for i := 0; i < len(vals); i++ {
		for j := 0; j < len(vals[i]); j++ {
			if vals[i][j] != gvals[i][j] {
				t.Error("value mismatch at", i, j, vals[i][j], gvals[i][j])
				for k := 0; k < len(vals); k++ {
					fmt.Print(gvals[k], " ")
				}
				fmt.Println("")
				return
			}
		}
	}
}
