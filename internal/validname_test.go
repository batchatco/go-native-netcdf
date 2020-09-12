package internal

import "testing"

func TestGood(t *testing.T) {
	var goodStrings = []string{
		"_",
		"a",
		"1",
		"0°",
		"byter",
	}
	for i := range goodStrings {
		if !IsValidNetCDFName(goodStrings[i]) {
			t.Error("name should be good", goodStrings[i])
			return
		}
	}
}

func TestBad(t *testing.T) {
	var badStrings = []string{
		"_ ",
		"/",
		"no/good",
		"\ta ",
		"1\t",
		"°",
		"°C",
		"\x08",
		"byte", "char", "string",
		"short", "int", "int64", "int64",
		"ushort", "uint", "uint64", "uint64",
		"float", "double",
	}
	for i := range badStrings {
		if IsValidNetCDFName(badStrings[i]) {
			t.Error("name should be bad", badStrings[i])
			return
		}
	}
}
