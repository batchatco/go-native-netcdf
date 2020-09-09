package hdf5

import (
	"testing"
)

func TestListTypes(t *testing.T) {
	type expected map[string]string
	expAll := map[string]expected{
		"testarray": {
			"comp": "compound { int(3) iArray; float(2,3) fArray; };",
		},
		"testattrs": {
			"alltypes": "compound { byte b; short s; int i; float f; double d; };",
			"color":    "byte enum { RED = 0, YELLOW = 1, GREEN = 2, CYAN = 3, BLUE = 4, MAGENTA = 5 }",
		},
		"testcompounds": {

			"alltypes":  "compound { byte b; short s; int i; float f; double d; };",
			"sametypes": "compound { int a; int b; int c; };",
			"includes":  "compound { alltypes a; string s; };",
		},
		"testempty": {
			"opaque5":  "opaque(5)",
			"alltypes": "compound { byte b; short s; int i; float f; double d; };",
		},
		"testenum": {
			"color": "byte enum { RED = 0, YELLOW = 1, GREEN = 2, CYAN = 3, BLUE = 4, MAGENTA = 5 }",
			"junk":  "int64 enum { FIRST = 1, SECOND = 2, THIRD = 3, FOURTH = 4, FIFTH = 5, SIXTH = 6 }",
		},
		"testopaque": {
			"opaque5": "opaque(5)",
		},
		"testsimple": {
			"AAA": "compound { short s; int i; };",
			"BBB": "compound { float x; double y; };",
		},
		"testvlen": {
			"vint": "int(*)",
			"easy": "compound { int firstEasy; int secondEasy; };",
			"easyVlen": "easy(*)",
			"tricky": "compound { int trickyInt; easyVlen trickVlen; };",
		},
	}
	for fileName, m := range expAll {
		func(m expected) {
			genName := ncGen(t, fileName)
			if genName == "" {
				t.Error(errorNcGen)
				return
			}
			nc, err := Open(genName)
			if err != nil {
				t.Error(err)
				return
			}
			defer nc.Close()
			h5, _ := nc.(*HDF5)
			types := h5.listTypes()
			hasMap := map[string]bool{}
			for _, typeName := range types {
				tVal, has := m[typeName]
				if !has {
					t.Error(fileName, "missing", typeName)
					continue
				}
				hasMap[typeName] = true
				val := h5.getType(typeName)
				if val != tVal {
					t.Errorf("%s: type mismatch got=(%s) exp=(%s)", fileName, val, tVal)
				}
			}
			for typeName := range m {
				if !hasMap[typeName] {
					t.Error(fileName, "has extra type", typeName)
				}
			}
		}(m)
	}
}

func TestGoTypes(t *testing.T) {
	type expected map[string]string
	expAll := map[string]expected{
		// Precede the type name with "type"
		"testarray": {
			"comp": "type comp struct {\n\tiArray [3]int32\n\tfArray [2][3]float32\n}\n",
		},
		"testattrs": {
			"alltypes": "type alltypes struct {\n\tb int8\n\ts int16\n\ti int32\n\tf float32\n\td float64\n}\n",
			"color":    "type color int8\nconst (\n\tRED color = 0\n\tYELLOW = 1\n\tGREEN = 2\n\tCYAN = 3\n\tBLUE = 4\n\tMAGENTA = 5\n)\n",
		},
		"testcompounds": {

			"alltypes":  "type alltypes struct {\n\tb int8\n\ts int16\n\ti int32\n\tf float32\n\td float64\n}\n",
			"sametypes": "type sametypes struct {\n\ta int32\n\tb int32\n\tc int32\n}\n",
			"includes":  "type includes struct {\n\ta alltypes\n\ts string\n}\n",
		},
		"testempty": {
			"opaque5":  "type opaque5 [5]uint8",
			"alltypes": "type alltypes struct {\n\tb int8\n\ts int16\n\ti int32\n\tf float32\n\td float64\n}\n",
		},
		"testenum": {
			"color": "type color int8\nconst (\n\tRED color = 0\n\tYELLOW = 1\n\tGREEN = 2\n\tCYAN = 3\n\tBLUE = 4\n\tMAGENTA = 5\n)\n",
			"junk":  "type junk int64\nconst (\n\tFIRST junk = 1\n\tSECOND = 2\n\tTHIRD = 3\n\tFOURTH = 4\n\tFIFTH = 5\n\tSIXTH = 6\n)\n",
		},
		"testopaque": {
			"opaque5": "type opaque5 [5]uint8",
		},
		"testsimple": {
			"AAA": "type AAA struct {\n\ts int16\n\ti int32\n}\n",
			"BBB": "type BBB struct {\n\tx float32\n\ty float64\n}\n"},
		"testvlen": {
			"vint": "type vint []int32",
			"easy": "type easy struct {\n\tfirstEasy int32\n\tsecondEasy int32\n}\n",
			"easyVlen": "type easyVlen []easy",
			"tricky": "type tricky struct {\n\ttrickyInt int32\n\ttrickVlen easyVlen\n}\n",
		},
	}
	for fileName, m := range expAll {
		func(m expected) {
			genName := ncGen(t, fileName)
			if genName == "" {
				t.Error(errorNcGen)
				return
			}
			nc, err := Open(genName)
			if err != nil {
				t.Error(err)
				return
			}
			defer nc.Close()
			h5, _ := nc.(*HDF5)
			types := h5.listTypes()
			hasMap := map[string]bool{}
			for _, typeName := range types {
				tVal, has := m[typeName]
				if !has {
					t.Error(fileName, "missing", typeName)
					continue
				}
				hasMap[typeName] = true
				val := h5.getGoType(typeName)
				if val != tVal {
					t.Errorf("%s: type mismatch got=(%s) exp=(%s)", fileName, val, tVal)
				}
			}
			for typeName := range m {
				if !hasMap[typeName] {
					t.Error(fileName, "has extra type", typeName)
				}
			}
			vars := h5.ListVariables()
			for _, varName := range vars {
				val := h5.getGoType(varName)
				if val != "" {
					t.Errorf("%s: variables are not types got=(%s)", fileName, val)
				}
			}
		}(m)
	}
}
