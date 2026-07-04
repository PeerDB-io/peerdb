package gen

import (
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"testing"
)

var testFreshNames = []string{
	"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8",
	"first2", "after2", "period2", "system2", "vector2",
	"new`tick", "spelled name", "имя2",
}

var testFixtureColumns = []string{
	"id", "c_int", "c_tiny", "c_dec", "c_dbl", "c_vchar", "c_text", "c_vbin",
	"c_blob", "c_date", "c_dt", "c_ts", "c_time", "c_year", "c_enum", "c_set",
	"c_json", "c_bit", "c_geom", "first", "after", "period", "system", "vector",
	"back`tick", "имя_utf8",
}

func TestFreshIdentAvoidsSnapshotAndReusesPaletteAfterReset(t *testing.T) {
	c := &Ctx{
		R: rand.New(rand.NewPCG(1, 2)),
		V: Vocab{Columns: slices.Clone(testFreshNames), FreshNames: slices.Clone(testFreshNames)},
	}
	seen := map[string]bool{}
	cols := boolSet(testFreshNames)
	for range 10 {
		name := c.freshIdent()
		if cols[name] {
			t.Fatalf("freshIdent returned snapshot column %q", name)
		}
		if seen[name] {
			t.Fatalf("freshIdent returned duplicate synthesized name %q", name)
		}
		seen[name] = true
	}

	free := testFreshNames[len(testFreshNames)-1]
	c = &Ctx{
		R: rand.New(rand.NewPCG(3, 4)),
		V: Vocab{Columns: slices.Clone(testFreshNames[:len(testFreshNames)-1]), FreshNames: slices.Clone(testFreshNames)},
	}
	if got := c.freshIdent(); got != free {
		t.Fatalf("freshIdent with one free palette name = %q, want %q", got, free)
	}

	c = &Ctx{
		R: rand.New(rand.NewPCG(5, 6)),
		V: Vocab{Columns: []string{"id", "c_int"}, FreshNames: slices.Clone(testFreshNames)},
	}
	for range len(testFreshNames) {
		if got := c.freshIdent(); !slices.Contains(testFreshNames, got) {
			t.Fatalf("fresh Ctx returned synthesized name before palette exhausted: %q", got)
		}
	}
}

func TestGenerateConstrainedAddTupleUsesDistinctFreshNames(t *testing.T) {
	v := Vocab{
		Table:      "fixture",
		Columns:    slices.Clone(testFixtureColumns),
		FreshNames: []string{"n1"},
		Types:      []string{"int"},
	}
	p := Profile{HeadsAlterOnly: true, NoAlterRenameTo: true, NoConvertCharset: true, MaxSpecs: 8}
	r := rand.New(rand.NewPCG(11, 12))
	addTuple := regexp.MustCompile(`ADD \((` + identPattern + `) int, (` + identPattern + `) int\)`)
	found := 0
	for range 5000 {
		sql := GenerateConstrained(r, v, p)
		for _, m := range addTuple.FindAllStringSubmatch(sql, -1) {
			found++
			left, right := unquoteTestIdent(m[1]), unquoteTestIdent(m[2])
			if left == right {
				t.Fatalf("ADD tuple reused fresh name %q in %q", left, sql)
			}
		}
	}
	if found == 0 {
		t.Fatal("no ADD tuple generated")
	}
}

func TestGenerateConstrainedFreshPositionsAvoidSnapshotColumns(t *testing.T) {
	columns := append(slices.Clone(testFixtureColumns), testFreshNames...)
	v := Vocab{
		Table:      "fixture",
		Columns:    columns,
		FreshNames: slices.Clone(testFreshNames),
		Types:      []string{"int"},
	}
	p := Profile{HeadsAlterOnly: true, NoAlterRenameTo: true, NoConvertCharset: true, MaxSpecs: 8}
	r := rand.New(rand.NewPCG(21, 22))
	live := boolSet(columns)
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`ADD COLUMN\s+(` + identPattern + `)`),
		regexp.MustCompile(`ADD \(\s*(` + identPattern + `)`),
		regexp.MustCompile(`ADD \(\s*` + identPattern + `\s+int,\s+(` + identPattern + `)`),
		regexp.MustCompile(`CHANGE COLUMN\s+` + identPattern + `\s+(` + identPattern + `)`),
		regexp.MustCompile(`RENAME COLUMN\s+` + identPattern + `\s+TO\s+(` + identPattern + `)`),
	}

	found := 0
	for range 5000 {
		sql := GenerateConstrained(r, v, p)
		for _, re := range patterns {
			for _, m := range re.FindAllStringSubmatch(sql, -1) {
				found++
				name := unquoteTestIdent(m[1])
				if live[name] {
					t.Fatalf("fresh position used live column %q in %q", name, sql)
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("no fresh positions generated")
	}
}

func TestGenAttrsSimpleTypeAwareDefaults(t *testing.T) {
	stringTypes := []string{"varchar(32)", "char(4)", "varbinary(16)", "binary(8)"}
	numericTypes := []string{"int", "bigint", "tinyint(1)", "decimal(10,2)", "double", "bit(8)"}
	baseOnlyTypes := []string{"text", "blob", "json", "enum('a','b')", "date", "datetime(3)", "timestamp(6) NULL", "time", "year"}
	baseMySQL := boolSet([]string{"", " NOT NULL", " NULL", " DEFAULT NULL", " COMMENT 'ddlfuzz'", " INVISIBLE", " VISIBLE"})
	baseMariaDB := boolSet([]string{"", " NOT NULL", " NULL", " DEFAULT NULL", " COMMENT 'ddlfuzz'", " INVISIBLE"})

	for _, isMaria := range []bool{false, true} {
		t.Run(map[bool]string{false: "mysql", true: "mariadb"}[isMaria], func(t *testing.T) {
			base := baseMySQL
			if isMaria {
				base = baseMariaDB
			}
			c := &Ctx{R: rand.New(rand.NewPCG(31, uint64(map[bool]uint64{false: 32, true: 33}[isMaria]))), IsMariaDB: isMaria}
			for _, typ := range append(append(slices.Clone(stringTypes), numericTypes...), baseOnlyTypes...) {
				for range 1000 {
					attr := genAttrsSimple(c, typ)
					if isMaria && attr == " VISIBLE" {
						t.Fatalf("MariaDB attr for %q = VISIBLE", typ)
					}
					switch {
					case slices.Contains(stringTypes, typ):
						if attr != " DEFAULT 'x,y'" && !base[attr] {
							t.Fatalf("string attr for %q = %q", typ, attr)
						}
					case slices.Contains(numericTypes, typ):
						if attr != " DEFAULT 1" && !base[attr] {
							t.Fatalf("numeric attr for %q = %q", typ, attr)
						}
						if attr == " DEFAULT 'x,y'" {
							t.Fatalf("numeric attr for %q got string default", typ)
						}
					default:
						if !base[attr] {
							t.Fatalf("base-only attr for %q = %q", typ, attr)
						}
					}
				}
			}
		})
	}
}

const identPattern = "(?:`(?:``|[^`])*`|\"(?:\"\"|[^\"])*\"|\\[(?:\\]\\]|[^\\]])*\\]|[[:alnum:]_]+)"

func boolSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func unquoteTestIdent(s string) string {
	if strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	}
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return strings.ReplaceAll(s[1:len(s)-1], "]]", "]")
	}
	return s
}
