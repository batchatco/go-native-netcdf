package internal

import "testing"

func TestGood(t *testing.T) {
	var goodStrings = []string{
		"a",
		"1",
		"0°",
		"byter",
		// Type names are valid NetCDF names (only reserved in CDL syntax)
		"byte", "char", "string",
		"short", "int", "int64",
		"ushort", "uint", "uint64",
		"float", "double",
		"enum", "opaque", "compound",
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
		// Names starting with underscore are reserved for system use
		"_",
		"_FillValue",
		"_NCProperties",
	}
	for i := range badStrings {
		if IsValidNetCDFName(badStrings[i]) {
			t.Error("name should be bad", badStrings[i])
			return
		}
	}
}
