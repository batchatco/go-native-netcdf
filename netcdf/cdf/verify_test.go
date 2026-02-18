// Verify that ncdump can read the files we write
package cdf

import (
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/batchatco/go-native-netcdf/netcdf/util"
)

func getFiles(t *testing.T, path string, suffix string) map[string]bool {
	names := make(map[string]bool)
	files, err := os.ReadDir(path)
	if err != nil {
		t.Log("Error opening", path, err)
		return nil
	}
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	for _, file := range files {
		fullName := path + file.Name()
		if file.IsDir() {
			more := getFiles(t, fullName, suffix)
			for m := range more {
				names[m] = true
			}
		} else if strings.HasSuffix(file.Name(), suffix) {
			names[fullName] = true
		}
	}
	return names
}

func ncDump(t *testing.T, fname string) (success bool) {
	t.Helper()
	cmdString := []string{"-h", fname}
	cmd := exec.Command("ncdump", cmdString...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Log("ncdump error (exec)", err)
		return true
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Log("ncdump error (exec)", err)
		return true
	}
	err = cmd.Start()
	success = true
	defer func() {
		err := cmd.Wait()
		if err != nil {
			t.Log("ncdump error (wait)", err)
			success = false
		}
	}()
	if err != nil {
		t.Log("ncdump error (start)", err)
		return true
	}
	for {
		var b [1024 * 1024]byte
		_, err := stdout.Read(b[:])
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		t.Log(err)
		return false
	}
	errText, r := io.ReadAll(stderr)
	if r == nil {
		if string(errText) != "" {
			t.Log(string(errText))
		}
	} else {
		t.Log("readall err", r)
		return false
	}
	return true
}

func TestCompat(t *testing.T) {
	fileNames := getFiles(t, "testdata", ".cdl")
gettingfiles:
	for fileName := range fileNames {
		baseName := fileName[:len(fileName)-4]
		genName := ncGen(t, baseName)
		if genName == "" {
			t.Error(errorNcGen)
			continue
		}
		defer os.Remove(genName)
		t.Log("TEST:", genName)
		nc, err := Open(genName)
		if err != nil {
			t.Error(err)
			continue
		}
		wname := baseName + "-written.nc"
		_ = os.Remove(wname)
		defer os.Remove(wname)
		cw, err := OpenWriter(wname)
		if err != nil {
			t.Error(err)
			continue
		}
		newKeys := make([]string, 0)
		newValues := make(map[string]any)
		gattr := nc.Attributes()
		for _, attrName := range gattr.Keys() {
			v, _ := gattr.Get(attrName)
			newKeys = append(newKeys, attrName)
			newValues[attrName] = v
		}
		newAttrs, err := util.NewOrderedMap(newKeys, newValues)
		if err != nil {
			t.Error(err)
			return
		}
		cw.AddAttributes(newAttrs)
		for _, varName := range nc.ListVariables() {
			val, err := nc.GetVariable(varName)
			if err != nil {
				t.Error(err)
				continue gettingfiles
			}
			err = cw.AddVar(varName, *val)
			if err != nil {
				t.Error(err)
				continue gettingfiles
			}
		}
		cw.Close()
		nc.Close()
		if !ncDump(t, wname) {
			t.Error("can't ncdump", wname)
			continue
		}
	}
}
