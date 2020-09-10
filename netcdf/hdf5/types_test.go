package hdf5

import (
	"testing"
)

func TestListTypes(t *testing.T) {
	type expected map[string]string
	expAll := map[string]expected{
		"testarray": {
			"comp": "compound {\n\tint(3) iArray;\n\tfloat(2,3) fArray;\n};",
		},
		"testattrs": {
			"alltypes": "compound {\n\tbyte b;\n\tshort s;\n\tint i;\n\tfloat f;\n\tdouble d;\n};",
			"color":    "byte enum {\n\tRED = 0,\n\tYELLOW = 1,\n\tGREEN = 2,\n\tCYAN = 3,\n\tBLUE = 4,\n\tMAGENTA = 5\n}",
		},
		"testcompounds": {

			"alltypes":  "compound {\n\tbyte b;\n\tshort s;\n\tint i;\n\tfloat f;\n\tdouble d;\n};",
			"sametypes": "compound {\n\tint a;\n\tint b;\n\tint c;\n};",
			"includes":  "compound {\n\talltypes a;\n\tstring s;\n};",
		},
		"testempty": {
			"opaque5":  "opaque(5)",
			"alltypes": "compound {\n\tbyte b;\n\tshort s;\n\tint i;\n\tfloat f;\n\tdouble d;\n};",
		},
		"testenum": {
			"color": "byte enum {\n\tRED = 0,\n\tYELLOW = 1,\n\tGREEN = 2,\n\tCYAN = 3,\n\tBLUE = 4,\n\tMAGENTA = 5\n}",
			"junk":  "int64 enum {\n\tFIRST = 1,\n\tSECOND = 2,\n\tTHIRD = 3,\n\tFOURTH = 4,\n\tFIFTH = 5,\n\tSIXTH = 6\n}",
		},
		"testgroups": {},
		"testopaque": {
			"opaque5": "opaque(5)",
		},
		"testsimple": {
			"AAA": "compound {\n\tshort s;\n\tint i;\n};",
			"BBB": "compound {\n\tfloat x;\n\tdouble y;\n};",
		},
		"testvlen": {
			"vint":     "int(*)",
			"easy":     "compound {\n\tint firstEasy;\n\tint secondEasy;\n};",
			"easyVlen": "easy(*)",
			"tricky":   "compound {\n\tint trickyInt;\n\teasyVlen trickVlen;\n};",
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
			types := nc.ListTypes()
			hasMap := map[string]bool{}
			for _, typeName := range types {
				tVal, has := m[typeName]
				if !has {
					t.Error(fileName, "missing", typeName)
					continue
				}
				hasMap[typeName] = true
				val, has := nc.GetType(typeName)
				if !has || val != tVal {
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
		"testgroups": {},
		"testopaque": {
			"opaque5": "type opaque5 [5]uint8",
		},
		"testsimple": {
			"AAA": "type AAA struct {\n\ts int16\n\ti int32\n}\n",
			"BBB": "type BBB struct {\n\tx float32\n\ty float64\n}\n"},
		"testvlen": {
			"vint":     "type vint []int32",
			"easy":     "type easy struct {\n\tfirstEasy int32\n\tsecondEasy int32\n}\n",
			"easyVlen": "type easyVlen []easy",
			"tricky":   "type tricky struct {\n\ttrickyInt int32\n\ttrickVlen easyVlen\n}\n",
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
			types := nc.ListTypes()
			hasMap := map[string]bool{}
			for _, typeName := range types {
				tVal, has := m[typeName]
				if !has {
					t.Error(fileName, "missing", typeName)
					continue
				}
				hasMap[typeName] = true
				val, has := nc.GetGoType(typeName)
				if !has {
					t.Error("type", typeName, "not found")
					continue
				}
				if val != tVal {
					t.Errorf("%s: type mismatch got=(%s) exp=(%s)", fileName, val, tVal)
					continue
				}
			}
			for typeName := range m {
				if !hasMap[typeName] {
					t.Error(fileName, "has extra type", typeName)
				}
			}
			vars := nc.ListVariables()
			for _, varName := range vars {
				val, has := nc.GetGoType(varName)
				if has {
					t.Errorf("%s: variables are not types got=(%s)", fileName, val)
				}
			}
		}(m)
	}
}
