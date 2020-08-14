// Used by go:generate to compile the cdl files into nc files
package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/batchatco/go-thrower"
)

func getFiles(path string, suffix string) []string {
	names := make([]string, 0)
	files, err := ioutil.ReadDir(path)
	thrower.ThrowIfError(err)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffix) {
			base := file.Name()[:len(file.Name())-len(suffix)]
			names = append(names, base)
		}
	}
	return names
}

func doit() (err error) {
	defer thrower.RecoverError(&err)
	err = os.Chdir("testdata")
	thrower.ThrowIfError(err)
	bases := getFiles(".", ".cdl")
	for _, base := range bases {
		cmd := exec.Command("ncgen", "-b", "-k", "hdf5", base+".cdl")
		err := cmd.Run()
		thrower.ThrowIfError(err)
	}
	return nil
}

func main() {
	err := doit()
	if err != nil {
		log.Fatal("An error occurred", err)
	}
}
