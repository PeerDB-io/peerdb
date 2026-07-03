package gen

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

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

// GenerateConstrained emits one DDL statement drawn from v under p. (Full grammar
// tables per 21-fuzzer.md step 12; the unconstrained generator shares them.)
func GenerateConstrained(r *rand.Rand, v Vocab, p Profile) string {
	if r == nil {
		r = rand.New(rand.NewPCG(1, 2))
	}
	v = normalizeVocab(v)
	maxSpecs := p.MaxSpecs
	if maxSpecs <= 0 {
		maxSpecs = 3
	}
	if maxSpecs > 8 {
		maxSpecs = 8
	}
	if !p.HeadsAlterOnly && p.RenameTableWeight > 0 && r.Float64() < p.RenameTableWeight {
		return genRename(r, v)
	}
	specN := 1 + r.IntN(maxSpecs)
	specs := make([]string, 0, specN)
	for i := 0; i < specN; i++ {
		specs = append(specs, genAlterSpec(r, v, p))
	}
	head := "ALTER TABLE "
	if v.IsMariaDB && r.IntN(20) == 0 {
		head = "ALTER ONLINE TABLE "
	} else if r.IntN(30) == 0 {
		head = "ALTER IGNORE TABLE "
	}
	return head + quoteMaybe(r, v.Table) + " " + strings.Join(specs, ", ")
}

func Generate(r *rand.Rand, isMariaDB bool) string {
	v := Vocab{
		Table:      pick(r, []string{"t", "orders", "db.t", "世界"}),
		Columns:    []string{"c", "a", "b", "old_col", "vector", "period"},
		FreshNames: []string{"c2", "new_col", "after_col", "世界_col", "json_col"},
		Types:      defaultTypes(isMariaDB),
		IsMariaDB:  isMariaDB,
	}
	return GenerateConstrained(r, v, Profile{RenameTableWeight: 0.08, MaxSpecs: 5})
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

func genRename(r *rand.Rand, v Vocab) string {
	dst := pick(r, v.FreshNames)
	if dst == "" || dst == v.Table {
		dst = v.Table + "_renamed"
	}
	return fmt.Sprintf("RENAME TABLE %s TO %s", quoteMaybe(r, v.Table), quoteMaybe(r, dst))
}

func genAlterSpec(r *rand.Rand, v Vocab, p Profile) string {
	col := quoteMaybe(r, pick(r, v.Columns))
	fresh := quoteMaybe(r, pick(r, v.FreshNames))
	typ := pick(r, v.Types)
	switch r.IntN(12) {
	case 0, 1, 2:
		pos := ""
		if r.IntN(4) == 0 {
			pos = " AFTER " + quoteMaybe(r, pick(r, v.Columns))
		} else if r.IntN(12) == 0 {
			pos = " FIRST"
		}
		return "ADD COLUMN " + fresh + " " + typ + genAttrs(r) + pos
	case 3:
		return "ADD (" + fresh + " " + typ + ", " + quoteMaybe(r, pick(r, v.FreshNames)) + " " + pick(r, v.Types) + ")"
	case 4, 5:
		return "MODIFY COLUMN " + col + " " + typ + genAttrs(r)
	case 6, 7:
		return "CHANGE COLUMN " + col + " " + fresh + " " + typ + genAttrs(r)
	case 8:
		return "DROP COLUMN " + col
	case 9:
		return "RENAME COLUMN " + col + " TO " + fresh
	case 10:
		if p.NoConvertCharset {
			return "ADD INDEX idx_" + bareIdentifier(col) + " (" + col + ")"
		}
		return pick(r, []string{
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
		return pick(r, []string{
			"RENAME TO " + quoteMaybe(r, pick(r, v.FreshNames)),
			"ENGINE=InnoDB",
			"ORDER BY " + col,
			"DISABLE KEYS",
			"ENABLE KEYS",
		})
	}
}

func genAttrs(r *rand.Rand) string {
	attrs := []string{"", " NOT NULL", " NULL", " DEFAULT NULL", " DEFAULT 'x,y'", " COMMENT 'ddlfuzz'", " INVISIBLE", " VISIBLE"}
	return pick(r, attrs)
}

func defaultTypes(isMariaDB bool) []string {
	types := []string{
		"INT", "INT UNSIGNED", "TINYINT(1)", "BIGINT", "DECIMAL", "DECIMAL(12)", "DECIMAL(10,2)",
		"FLOAT", "DOUBLE", "VARCHAR(32)", "TEXT", "JSON", "BLOB", "DATETIME(6)", "TIMESTAMP",
		"ENUM('a','b')", "SET('x','y')", "GEOMETRY", "POINT", "VECTOR(3)",
	}
	if isMariaDB {
		types = append(types, "UUID", "INET4", "NUMBER(10)", "VARCHAR2(30)", "RAW(16)", "CLOB")
	}
	return types
}

func quoteMaybe(r *rand.Rand, ident string) string {
	if strings.Contains(ident, ".") {
		parts := strings.Split(ident, ".")
		for i := range parts {
			parts[i] = quoteMaybe(r, parts[i])
		}
		return strings.Join(parts, ".")
	}
	if r.IntN(5) == 0 || needsQuote(ident) {
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}
	return ident
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

func pick(r *rand.Rand, xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	if r == nil {
		return xs[0]
	}
	return xs[r.IntN(len(xs))]
}
