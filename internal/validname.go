package internal

import (
	"regexp"
)

const (
	// A valid name must start with a letter, digit or underscore.
	// It may contain any character after that except control and slash.
	pattern = `^[\pL\pN_][^\pC/]*$`
	// It may not end with a whitespace character, or be a reserved word.
	antiPattern = `(\pZ|^(u?byte|char|string|u?short|u?int|u?int64|uint64|float|double|enum|opaque|compound))$`
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
