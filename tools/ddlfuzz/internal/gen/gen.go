package gen

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"unicode"
)

const (
	ModeANSIQuotes         uint64 = 1 << 2
	ModeOracle             uint64 = 1 << 9
	ModeMSSQL              uint64 = 1 << 10
	ModeNoBackslashEscapes uint64 = 1 << 20

	sentinelDB = "peerdb_ddlfuzz_nodb"
)

type Ctx struct {
	R         *rand.Rand
	IsMariaDB bool
	Mode      uint64
	V         Vocab

	tableSuffix string
}

// Vocab is the fixture vocabulary the constrained (e2e) profile draws from.
type Vocab struct {
	Table      string
	Columns    []string
	FreshNames []string
	Types      []string
	IsMariaDB  bool
}

// Profile constrains GenerateConstrained for the e2e lane.
type Profile struct {
	HeadsAlterOnly    bool
	NoAlterRenameTo   bool
	NoConvertCharset  bool
	RenameTableWeight float64
	MaxSpecs          int
}

func ChooseMode(r *rand.Rand, isMariaDB bool) uint64 {
	if r == nil {
		r = rand.New(rand.NewPCG(1, 2))
	}
	modes := []uint64{ModeANSIQuotes, ModeNoBackslashEscapes}
	if isMariaDB {
		modes = append(modes, ModeOracle, ModeMSSQL)
	}
	x := r.IntN(100)
	switch {
	case x < 50:
		return 0
	case x < 80:
		return modes[r.IntN(len(modes))]
	default:
		a := modes[r.IntN(len(modes))]
		b := modes[r.IntN(len(modes))]
		for len(modes) > 1 && b == a {
			b = modes[r.IntN(len(modes))]
		}
		return a | b
	}
}

func GenerateConstrained(r *rand.Rand, v Vocab, p Profile) string {
	if r == nil {
		r = rand.New(rand.NewPCG(1, 2))
	}
	v = normalizeVocab(v)
	ctx := &Ctx{R: r, IsMariaDB: v.IsMariaDB, V: v}
	maxSpecs := p.MaxSpecs
	if maxSpecs <= 0 {
		maxSpecs = 3
	}
	if maxSpecs > 8 {
		maxSpecs = 8
	}
	if !p.HeadsAlterOnly && p.RenameTableWeight > 0 && r.Float64() < p.RenameTableWeight {
		return genRename(ctx)
	}
	specN := 1 + r.IntN(maxSpecs)
	specs := make([]string, 0, specN)
	for i := 0; i < specN; i++ {
		specs = append(specs, genAlterSpec(ctx, p))
	}
	head := "ALTER TABLE "
	if v.IsMariaDB && r.IntN(20) == 0 {
		head = "ALTER ONLINE TABLE "
	} else if r.IntN(30) == 0 {
		head = "ALTER IGNORE TABLE "
	}
	return head + quoteMaybe(ctx, v.Table) + " " + strings.Join(specs, ", ")
}

func Generate(r *rand.Rand, isMariaDB bool, mode uint64) string {
	if r == nil {
		r = rand.New(rand.NewPCG(3, 4))
	}
	ctx := &Ctx{R: r, IsMariaDB: isMariaDB, Mode: mode}
	ctx.V = normalizeVocab(randomVocab(ctx))
	n := 1
	if r.IntN(100) < 18 {
		n += r.IntN(3)
	}
	stmts := make([]string, 0, n)
	for i := 0; i < n; i++ {
		stmts = append(stmts, genStatement(ctx))
	}
	sql := strings.Join(stmts, "; ")
	if isMariaDB && r.IntN(100) < 4 {
		sql = genSetStatement(ctx, sql)
	}
	if r.IntN(100) < 15 {
		sql = decorate(ctx, sql)
	}
	if r.IntN(100) == 0 {
		sql += "\x00"
	}
	if r.IntN(100) == 0 {
		sql = strings.Replace(sql, "'ddlfuzz'", "'ddl\x00fuzz'", 1)
	}
	if strings.Contains(sql, sentinelDB) {
		return "ALTER TABLE t ADD COLUMN c INT"
	}
	return sql
}

func genStatement(c *Ctx) string {
	if c.R.IntN(100) < 5 {
		if e, ok := pick(c, benignHeads); ok {
			return e.Emit(c)
		}
	}
	if c.R.IntN(100) < 8 {
		return genRename(c)
	}
	specN := 1 + c.R.IntN(5)
	specs := make([]string, 0, specN)
	for i := 0; i < specN; i++ {
		if e, ok := pick(c, alterSpecs); ok {
			specs = append(specs, e.Emit(c))
		}
	}
	head := "ALTER TABLE"
	c.tableSuffix = ""
	if e, ok := pick(c, alterHeads); ok {
		head = e.Emit(c)
	}
	suffix := c.tableSuffix
	c.tableSuffix = ""
	return head + " " + quoteMaybe(c, c.V.Table) + suffix + " " + strings.Join(specs, ", ")
}

func normalizeVocab(v Vocab) Vocab {
	if v.Table == "" {
		v.Table = "t"
	}
	if len(v.Columns) == 0 {
		v.Columns = []string{"c", "a", "b"}
	}
	if len(v.FreshNames) == 0 {
		v.FreshNames = []string{"c_new", "d", "e"}
	}
	if len(v.Types) == 0 {
		v.Types = defaultTypes(v.IsMariaDB)
	}
	return v
}

func randomVocab(c *Ctx) Vocab {
	tables := []string{"t", "orders", "db.t", "db.1234", "1ea10", "tëst", "世界"}
	cols := []string{"c", "a", "b", "old_col", "first", "after", "period", "system", "vector", "key", "cölumn_ñ", "世界"}
	fresh := []string{"c2", "new_col", "after_col", "json_col", "select", "column", "table", "1ea10", "世界_col"}
	if c.R.IntN(4) == 0 {
		cols = append(cols, keywordName(c))
		fresh = append(fresh, keywordName(c))
	}
	return Vocab{
		Table:      pickString(c.R, tables),
		Columns:    cols,
		FreshNames: fresh,
		Types:      defaultTypes(c.IsMariaDB),
		IsMariaDB:  c.IsMariaDB,
	}
}

func genRename(c *Ctx) string {
	if e, ok := pick(c, renameForms); ok {
		return e.Emit(c)
	}
	dst := pickString(c.R, c.V.FreshNames)
	if dst == "" || dst == c.V.Table {
		dst = c.V.Table + "_renamed"
	}
	return fmt.Sprintf("RENAME TABLE %s TO %s", quoteMaybe(c, c.V.Table), quoteMaybe(c, dst))
}

func genAlterSpec(c *Ctx, p Profile) string {
	col := quoteMaybe(c, pickString(c.R, c.V.Columns))
	fresh := quoteMaybe(c, pickString(c.R, c.V.FreshNames))
	typ := pickString(c.R, c.V.Types)
	switch c.R.IntN(12) {
	case 0, 1, 2:
		pos := ""
		if c.R.IntN(4) == 0 {
			pos = " AFTER " + quoteMaybe(c, pickString(c.R, c.V.Columns))
		} else if c.R.IntN(12) == 0 {
			pos = " FIRST"
		}
		return "ADD COLUMN " + fresh + " " + typ + genAttrsSimple(c) + pos
	case 3:
		return "ADD (" + fresh + " " + typ + ", " + quoteMaybe(c, pickString(c.R, c.V.FreshNames)) + " " + pickString(c.R, c.V.Types) + ")"
	case 4, 5:
		return "MODIFY COLUMN " + col + " " + typ + genAttrsSimple(c)
	case 6, 7:
		return "CHANGE COLUMN " + col + " " + fresh + " " + typ + genAttrsSimple(c)
	case 8:
		return "DROP COLUMN " + col
	case 9:
		return "RENAME COLUMN " + col + " TO " + fresh
	case 10:
		if p.NoConvertCharset {
			return "ADD INDEX idx_" + bareIdentifier(col) + " (" + col + ")"
		}
		return pickString(c.R, []string{
			"ADD INDEX idx_" + bareIdentifier(col) + " (" + col + ")",
			"CONVERT TO CHARACTER SET utf8mb4",
			"ALGORITHM=INPLACE",
			"LOCK=NONE",
			"FORCE",
		})
	default:
		if p.NoAlterRenameTo {
			return "ADD CHECK (" + col + " IS NOT NULL)"
		}
		return pickString(c.R, []string{
			"RENAME TO " + quoteMaybe(c, pickString(c.R, c.V.FreshNames)),
			"ENGINE=InnoDB",
			"ORDER BY " + col,
			"DISABLE KEYS",
			"ENABLE KEYS",
		})
	}
}

func genAttrs(c *Ctx) string {
	n := c.R.IntN(5)
	if n == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		if e, ok := pick(c, columnAttrs); ok {
			b.WriteByte(' ')
			b.WriteString(e.Emit(c))
		}
	}
	return b.String()
}

func genAttrsSimple(c *Ctx) string {
	attrs := []string{"", " NOT NULL", " NULL", " DEFAULT NULL", " DEFAULT 'x,y'", " COMMENT 'ddlfuzz'", " INVISIBLE", " VISIBLE"}
	return pickString(c.R, attrs)
}

func quoteMaybe(c *Ctx, ident string) string {
	if strings.Contains(ident, ".") {
		parts := strings.Split(ident, ".")
		for i := range parts {
			parts[i] = quoteMaybe(c, parts[i])
		}
		return strings.Join(parts, ".")
	}
	if c == nil || c.R == nil {
		if needsQuote(ident) {
			return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
		}
		return ident
	}
	if !needsQuote(ident) {
		if isReservedIdent(c, ident) {
			if c.R.IntN(10) == 0 {
				return ident
			}
		} else if c.R.IntN(5) != 0 {
			return ident
		}
	}
	switch {
	case c.IsMariaDB && c.Mode&ModeMSSQL != 0 && c.R.IntN(4) == 0:
		return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
	case c.Mode&ModeANSIQuotes != 0 && c.R.IntN(3) == 0:
		return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
	default:
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}
}

func needsQuote(s string) bool {
	if s == "" {
		return true
	}
	for i, r := range s {
		if r == '_' || r >= '0' && r <= '9' && i > 0 || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			continue
		}
		return true
	}
	return false
}

func bareIdentifier(s string) string {
	s = strings.Trim(s, "`[]\"")
	s = strings.ReplaceAll(s, ".", "_")
	if s == "" {
		return "c"
	}
	return s
}

func keywordName(c *Ctx) string {
	for i := 0; i < 32; i++ {
		t := pickString(c.R, keywordTokens(c.IsMariaDB))
		ok := true
		for _, r := range t {
			if !unicode.IsLetter(r) || r > unicode.MaxASCII {
				ok = false
				break
			}
		}
		if ok && t != "" {
			return strings.ToLower(t)
		}
	}
	return "select"
}

func pickString(r *rand.Rand, xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	if r == nil {
		return xs[0]
	}
	return xs[r.IntN(len(xs))]
}
