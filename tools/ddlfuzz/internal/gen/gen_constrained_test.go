package gen

import (
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// constrainedAllowlist is the binding constrained-emission construct set.
// Widening it is a conscious diff: flip an entry's CE bit and add its ID here.
var constrainedAllowlist = []string{
	"head.alter_table", "head.alter_online", "head.alter_ignore", "head.if_exists", "head.wait_nowait",
	"spec.add_column", "spec.add_column_if_not_exists", "spec.add_column_list", "spec.modify_column",
	"spec.change_column", "spec.drop_column", "spec.rename_column", "spec.add_index", "spec.add_check",
	"spec.keys_toggle", "spec.order_by", "spec.rename_table", "spec.convert_charset", "spec.algorithm",
	"spec.lock", "spec.force", "spec.table_options",
	"attr.nullability", "attr.default", "attr.comment", "attr.visibility",
	"rename.table",
}

func TestConstrainedEligibleSetMatchesAllowlist(t *testing.T) {
	listed := map[string]bool{}
	for _, id := range constrainedAllowlist {
		if _, dup := listed[id]; dup {
			t.Fatalf("duplicate allowlist id %q", id)
		}
		listed[id] = false
	}
	for _, e := range allEntries() {
		_, inList := listed[e.ID]
		if e.CE && !inList {
			t.Errorf("entry %q is CE but missing from constrainedAllowlist", e.ID)
		}
		if !e.CE && inList {
			t.Errorf("entry %q is in constrainedAllowlist but not CE", e.ID)
		}
		if inList {
			listed[e.ID] = true
		}
	}
	for id, seen := range listed {
		if !seen {
			t.Errorf("allowlist id %q has no entry", id)
		}
	}
}

func testModePalette(isMaria bool) []uint64 {
	modes := []uint64{0, ModeANSIQuotes, ModeNoBackslashEscapes, ModeANSIQuotes | ModeNoBackslashEscapes}
	if isMaria {
		modes = append(modes, ModeOracle, ModeMSSQL, ModeMSSQL|ModeNoBackslashEscapes)
	}
	return modes
}

func constrainedTestVocab(isMaria bool) Vocab {
	return Vocab{
		Table:      "fixture",
		Columns:    slices.Clone(testFixtureColumns),
		FreshNames: slices.Clone(testFreshNames),
		Types:      []string{"int", "varchar(32)"},
		IsMariaDB:  isMaria,
	}
}

func TestGenerateConstrainedShapeSet(t *testing.T) {
	const draws = 50000
	const typePat = `(?:int|varchar\(32\))`
	const attrPat = `(?: NOT NULL| NULL| DEFAULT NULL| DEFAULT 'x,y'| DEFAULT 1| COMMENT 'ddlfuzz'| INVISIBLE| VISIBLE)?`
	posPat := `(?: FIRST| AFTER ` + identPattern + `)?`
	iePat := `(?:IF EXISTS )?`
	specShapes := []struct {
		id string
		re *regexp.Regexp
	}{
		{"spec.add_column_if_not_exists", regexp.MustCompile(`^ADD COLUMN IF NOT EXISTS ` + identPattern + ` ` + typePat + attrPat + `$`)},
		{"spec.add_column_list", regexp.MustCompile(`^ADD \(` + identPattern + ` ` + typePat + `, ` + identPattern + ` ` + typePat + `, INDEX \(` + identPattern + `\)\)$`)},
		{"spec.add_column", regexp.MustCompile(`^ADD (?:COLUMN )?` + identPattern + ` ` + typePat + attrPat + posPat + `$`)},
		{"spec.modify_column", regexp.MustCompile(`^MODIFY (?:COLUMN )?` + iePat + identPattern + ` ` + typePat + attrPat + posPat + `$`)},
		{"spec.change_column", regexp.MustCompile(`^CHANGE (?:COLUMN )?` + iePat + identPattern + ` ` + identPattern + ` ` + typePat + attrPat + posPat + `$`)},
		{"spec.drop_column", regexp.MustCompile(`^DROP (?:COLUMN )?` + iePat + identPattern + `(?: RESTRICT| CASCADE)?$`)},
		{"spec.rename_column", regexp.MustCompile(`^RENAME COLUMN ` + iePat + identPattern + ` TO ` + identPattern + `$`)},
		{"spec.add_index", regexp.MustCompile(`^ADD (?:INDEX|KEY) (?:IF NOT EXISTS )?` + identPattern + ` (?:BTREE |HASH )?\(` + identPattern + `\)(?: KEY_BLOCK_SIZE=4| COMMENT 'idx,comment'| ALGORITHM=DEFAULT| LOCK=NONE| VISIBLE| INVISIBLE)?$`)},
		{"spec.add_check", regexp.MustCompile(`^ADD CHECK \(` + identPattern + ` IS NOT NULL\)$`)},
		{"spec.keys_toggle", regexp.MustCompile(`^(?:DISABLE|ENABLE) KEYS$`)},
		{"spec.order_by", regexp.MustCompile(`^ORDER BY ` + identPattern + `, ` + identPattern + `$`)},
		{"spec.rename_table", regexp.MustCompile(`^RENAME (?:TO|AS|=) ` + identPattern + `$`)},
		{"spec.convert_charset", regexp.MustCompile(`^CONVERT TO CHARACTER SET (?:utf8mb4 COLLATE utf8mb4_bin|DEFAULT)$`)},
		{"spec.algorithm", regexp.MustCompile(`^ALGORITHM=(?:DEFAULT|INSTANT|INPLACE|NOCOPY|COPY)$`)},
		{"spec.lock", regexp.MustCompile(`^LOCK=(?:DEFAULT|NONE|SHARED|EXCLUSIVE)$`)},
		{"spec.force", regexp.MustCompile(`^FORCE$`)},
		{"spec.table_options", regexp.MustCompile(`^ENGINE=InnoDB$`)},
	}
	headRe := regexp.MustCompile(`^ALTER (ONLINE |IGNORE )?TABLE (IF EXISTS )?` + identPattern + `( WAIT 5| NOWAIT)? (.+)$`)
	renameRe := regexp.MustCompile(`^RENAME TABLE ` + identPattern + ` TO ` + identPattern + `$`)

	for _, isMaria := range []bool{false, true} {
		t.Run(map[bool]string{false: "mysql", true: "mariadb"}[isMaria], func(t *testing.T) {
			modes := testModePalette(isMaria)
			v := constrainedTestVocab(isMaria)
			r := rand.New(rand.NewPCG(41, uint64(map[bool]uint64{false: 42, true: 43}[isMaria])))
			seen := map[string]bool{}
			for i := range draws {
				p := Profile{RenameTableWeight: 0.05, MaxSpecs: 1, Mode: modes[i%len(modes)]}
				sql := GenerateConstrained(r, v, p)
				if renameRe.MatchString(sql) {
					seen["rename.table"] = true
					continue
				}
				m := headRe.FindStringSubmatch(sql)
				if m == nil {
					t.Fatalf("statement outside constrained shapes: %q", sql)
				}
				switch {
				case m[1] == "ONLINE ":
					seen["head.alter_online"] = true
				case m[1] == "IGNORE ":
					seen["head.alter_ignore"] = true
				case m[2] != "":
					seen["head.if_exists"] = true
				case m[3] != "":
					seen["head.wait_nowait"] = true
				default:
					seen["head.alter_table"] = true
				}
				spec := m[4]
				specID := ""
				for _, s := range specShapes {
					if s.re.MatchString(spec) {
						specID = s.id
						break
					}
				}
				if specID == "" {
					t.Fatalf("spec outside constrained shapes: %q (in %q)", spec, sql)
				}
				seen[specID] = true
				if strings.Contains(sql, " NOT NULL") {
					seen["attr.nullability"] = true
				}
				if strings.Contains(sql, " DEFAULT NULL") || strings.Contains(sql, " DEFAULT 'x,y'") || strings.Contains(sql, " DEFAULT 1") {
					seen["attr.default"] = true
				}
				if strings.Contains(sql, "COMMENT 'ddlfuzz'") {
					seen["attr.comment"] = true
				}
				if strings.Contains(sql, "INVISIBLE") {
					seen["attr.visibility"] = true
				}
			}

			expected := []string{
				"head.alter_table",
				"spec.add_column", "spec.add_column_list", "spec.modify_column", "spec.change_column",
				"spec.drop_column", "spec.rename_column", "spec.add_index", "spec.add_check",
				"spec.keys_toggle", "spec.order_by", "spec.rename_table", "spec.convert_charset",
				"spec.algorithm", "spec.lock", "spec.force", "spec.table_options",
				"attr.nullability", "attr.default", "attr.comment", "attr.visibility",
				"rename.table",
			}
			if isMaria {
				expected = append(expected, "head.alter_online", "head.alter_ignore", "head.if_exists", "head.wait_nowait", "spec.add_column_if_not_exists")
			}
			for _, id := range expected {
				if !seen[id] {
					t.Errorf("CE construct %q never emitted in %d draws", id, draws)
				}
			}

			// Multi-spec assembly: spec boundaries are ambiguous to split, so
			// check the outer-shell invariants only.
			r2 := rand.New(rand.NewPCG(51, uint64(map[bool]uint64{false: 52, true: 53}[isMaria])))
			for i := range 5000 {
				p := Profile{RenameTableWeight: 0.05, MaxSpecs: 8, Mode: modes[i%len(modes)]}
				sql := GenerateConstrained(r2, v, p)
				if strings.ContainsAny(sql, ";\x00") || strings.Contains(sql, "SET STATEMENT") ||
					!(strings.HasPrefix(sql, "ALTER ") || strings.HasPrefix(sql, "RENAME TABLE ")) {
					t.Fatalf("multi-spec statement escaped the constrained shell: %q", sql)
				}
			}
		})
	}
}

func findTestEntry(t *testing.T, id string) entry {
	t.Helper()
	for _, e := range allEntries() {
		if e.ID == id {
			return e
		}
	}
	t.Fatalf("no entry %q", id)
	return entry{}
}

func newConstrainedTestCtx(isMaria bool, seed uint64) *Ctx {
	return &Ctx{
		R:         rand.New(rand.NewPCG(seed, seed+1)),
		IsMariaDB: isMaria,
		V:         normalizeVocab(constrainedTestVocab(isMaria)),
		P:         &Profile{},
	}
}

func TestConstrainedExecSafetyAuditFreshNames(t *testing.T) {
	freshCases := []struct {
		id        string
		mariaOnly bool
		re        *regexp.Regexp
	}{
		{"spec.add_column", false, regexp.MustCompile(`^ADD (?:COLUMN )?(` + identPattern + `) `)},
		{"spec.add_column_if_not_exists", true, regexp.MustCompile(`^ADD COLUMN IF NOT EXISTS (` + identPattern + `) `)},
		{"spec.add_column_list", false, regexp.MustCompile(`^ADD \((` + identPattern + `) [^,]+, (` + identPattern + `) `)},
		{"spec.change_column", false, regexp.MustCompile(`^CHANGE (?:COLUMN )?(?:IF EXISTS )?` + identPattern + ` (` + identPattern + `) `)},
		{"spec.rename_column", false, regexp.MustCompile(`^RENAME COLUMN (?:IF EXISTS )?` + identPattern + ` TO (` + identPattern + `)$`)},
		{"spec.rename_table", false, regexp.MustCompile(`^RENAME (?:TO|AS|=) (` + identPattern + `)$`)},
	}
	live := boolSet(testFixtureColumns)
	for _, isMaria := range []bool{false, true} {
		for _, tc := range freshCases {
			if tc.mariaOnly && !isMaria {
				continue
			}
			e := findTestEntry(t, tc.id)
			for iter := range 500 {
				// One Ctx per iteration mirrors one statement; multiple emissions
				// mirror a multi-spec statement sharing the fresh pool.
				c := newConstrainedTestCtx(isMaria, uint64(1000+iter*2))
				used := map[string]bool{}
				for range 3 {
					out := e.Emit(c)
					m := tc.re.FindStringSubmatch(out)
					if m == nil {
						t.Fatalf("%s emission did not match fresh-name shape: %q", tc.id, out)
					}
					for _, raw := range m[1:] {
						name := unquoteTestIdent(raw)
						if live[name] {
							t.Fatalf("%s used live column %q as fresh name in %q", tc.id, name, out)
						}
						if used[name] {
							t.Fatalf("%s reused fresh name %q within one Ctx in %q", tc.id, name, out)
						}
						used[name] = true
					}
				}
			}
		}
	}
}

func TestConstrainedExecSafetyAuditAttrs(t *testing.T) {
	for _, isMaria := range []bool{false, true} {
		c := newConstrainedTestCtx(isMaria, 2000)
		nullability := findTestEntry(t, "attr.nullability")
		comment := findTestEntry(t, "attr.comment")
		visibility := findTestEntry(t, "attr.visibility")
		def := findTestEntry(t, "attr.default")
		for range 2000 {
			if out := nullability.Emit(c); out != "NOT NULL" && out != "NULL" {
				t.Fatalf("isMaria=%v attr.nullability = %q", isMaria, out)
			}
			if out := comment.Emit(c); out != "COMMENT 'ddlfuzz'" {
				t.Fatalf("isMaria=%v attr.comment = %q", isMaria, out)
			}
			out := visibility.Emit(c)
			if isMaria && out != "INVISIBLE" {
				t.Fatalf("mariadb attr.visibility = %q", out)
			}
			if !isMaria && out != "VISIBLE" && out != "INVISIBLE" {
				t.Fatalf("mysql attr.visibility = %q", out)
			}
			for typ, allowed := range map[string][]string{
				"varchar(32)": {"DEFAULT NULL", "DEFAULT 'x,y'"},
				"int":         {"DEFAULT NULL", "DEFAULT 1"},
				"text":        {"DEFAULT NULL"},
			} {
				c.attrType = typ
				if out := def.Emit(c); !slices.Contains(allowed, out) {
					t.Fatalf("isMaria=%v attr.default for %q = %q", isMaria, typ, out)
				}
				c.attrType = ""
			}
		}
	}
}

func TestConstrainedExecSafetyAuditIndexAndOptions(t *testing.T) {
	checkRe := regexp.MustCompile(`^ADD CHECK \((` + identPattern + `) IS NOT NULL\)$`)
	live := boolSet(testFixtureColumns)
	for _, isMaria := range []bool{false, true} {
		c := newConstrainedTestCtx(isMaria, 3000)
		addIndex := findTestEntry(t, "spec.add_index")
		tblOpts := findTestEntry(t, "spec.table_options")
		addCheck := findTestEntry(t, "spec.add_check")
		for range 2000 {
			out := addIndex.Emit(c)
			if strings.Contains(out, "WITH PARSER") {
				t.Fatalf("isMaria=%v spec.add_index emitted WITH PARSER: %q", isMaria, out)
			}
			if isMaria && strings.Contains(out, "VISIBLE") {
				t.Fatalf("mariadb spec.add_index emitted index visibility: %q", out)
			}
			if out := tblOpts.Emit(c); out != "ENGINE=InnoDB" {
				t.Fatalf("isMaria=%v spec.table_options = %q", isMaria, out)
			}
			// No genExpr (NOW()/double-quoted literals), no CONSTRAINT sym
			// (dup name on the second draw), no ENFORCED (maria 1064).
			out = addCheck.Emit(c)
			m := checkRe.FindStringSubmatch(out)
			if m == nil {
				t.Fatalf("isMaria=%v spec.add_check = %q, want ADD CHECK (<live col> IS NOT NULL)", isMaria, out)
			}
			if col := unquoteTestIdent(m[1]); !live[col] {
				t.Fatalf("isMaria=%v spec.add_check referenced non-live column %q in %q", isMaria, col, out)
			}
		}
	}
}

func TestConstrainedExecSafetyAuditHeadsAndRenames(t *testing.T) {
	mysqlCtx := newConstrainedTestCtx(false, 4000)
	mariaCtx := newConstrainedTestCtx(true, 4100)
	for _, id := range []string{"head.alter_online", "head.alter_ignore", "head.if_exists", "head.wait_nowait"} {
		e := findTestEntry(t, id)
		if entryOK(mysqlCtx, e) {
			t.Errorf("%s eligible on constrained mysql", id)
		}
		if !entryOK(mariaCtx, e) {
			t.Errorf("%s not eligible on constrained mariadb", id)
		}
	}
	waitNowait := findTestEntry(t, "head.wait_nowait")
	for range 200 {
		mariaCtx.tableSuffix = ""
		if head := waitNowait.Emit(mariaCtx); head != "ALTER TABLE" {
			t.Fatalf("head.wait_nowait head = %q", head)
		}
		if s := mariaCtx.tableSuffix; s != " WAIT 5" && s != " NOWAIT" {
			t.Fatalf("head.wait_nowait suffix = %q", s)
		}
	}
	mariaCtx.tableSuffix = ""

	for _, id := range []string{"rename.tables", "rename.multi_pair", "rename.schema_qualified", "rename.wait_nowait"} {
		e := findTestEntry(t, id)
		if entryOK(mysqlCtx, e) || entryOK(mariaCtx, e) {
			t.Errorf("%s eligible under constrained profile", id)
		}
	}
	renameTable := findTestEntry(t, "rename.table")
	renameRe := regexp.MustCompile(`^RENAME TABLE (` + identPattern + `) TO (` + identPattern + `)$`)
	palette := boolSet(testFreshNames)
	for _, c := range []*Ctx{mysqlCtx, mariaCtx} {
		for range 500 {
			out := renameTable.Emit(c)
			m := renameRe.FindStringSubmatch(out)
			if m == nil {
				t.Fatalf("rename.table shape: %q", out)
			}
			if src := unquoteTestIdent(m[1]); src != "fixture" {
				t.Fatalf("rename.table source = %q in %q", src, out)
			}
			if dst := unquoteTestIdent(m[2]); !palette[dst] {
				t.Fatalf("rename.table target %q not from fresh palette in %q", dst, out)
			}
		}
	}
}

func TestGenerateConstrainedModeAudit(t *testing.T) {
	const draws = 10000
	for _, isMaria := range []bool{false, true} {
		for _, mode := range testModePalette(isMaria) {
			t.Run(fmt.Sprintf("maria=%v/mode=%#x", isMaria, mode), func(t *testing.T) {
				v := constrainedTestVocab(isMaria)
				p := Profile{RenameTableWeight: 0.05, MaxSpecs: 3, Mode: mode}
				r := rand.New(rand.NewPCG(61, mode^uint64(map[bool]uint64{false: 62, true: 63}[isMaria])))
				sawANSI, sawBracket := false, false
				for range draws {
					sql := GenerateConstrained(r, v, p)
					if strings.Contains(sql, `"`) {
						if mode&ModeANSIQuotes == 0 {
							t.Fatalf("ANSI-quoted identifier without ANSI_QUOTES bit: %q", sql)
						}
						sawANSI = true
					}
					if strings.Contains(sql, "[") {
						if !isMaria || mode&ModeMSSQL == 0 {
							t.Fatalf("bracket identifier without maria MSSQL bit: %q", sql)
						}
						sawBracket = true
					}
				}
				if mode&ModeANSIQuotes != 0 && !sawANSI {
					t.Errorf("no ANSI-quoted identifier in %d draws with ANSI_QUOTES set", draws)
				}
				if isMaria && mode&ModeMSSQL != 0 && !sawBracket {
					t.Errorf("no bracket identifier in %d draws with MSSQL set", draws)
				}
			})
		}
	}
}
