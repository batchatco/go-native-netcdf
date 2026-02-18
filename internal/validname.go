package internal

import (
	"regexp"
)

const (
	// A valid name must start with a letter or digit (not underscore,
	// which is reserved for system use like _FillValue, _NCProperties).
	// It may contain any character after that except control and slash.
	pattern = `^[\pL\pN][^\pC/]*$`
	// It may not end with a whitespace character.
	antiPattern = `\pZ$`
)

var (
	re     *regexp.Regexp
	antiRe *regexp.Regexp
)

func init() {
	var err error
	re, err = regexp.Compile(pattern)
	if err != nil {
		panic(err)
	}
	antiRe, err = regexp.Compile(antiPattern)
	if err != nil {
		panic(err)
	}
}

// IsValidNetCDFName returns true if name is a valid NetCDF name.
func IsValidNetCDFName(name string) bool {
	return re.MatchString(name) && !antiRe.MatchString(name)
}
