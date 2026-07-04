package seed

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	sqlModeANSIQuotes         uint64 = 1 << 2
	sqlModeOracle             uint64 = 1 << 9
	sqlModeMSSQL              uint64 = 1 << 10
	sqlModeNoBackslashEscapes uint64 = 1 << 20
)

type Seed struct {
	SQL       string `json:"sql"`
	Engine    string `json:"engine"`
	SQLMode   uint64 `json:"sql_mode"`
	ExpectSig string `json:"expect_sig,omitempty"`
	Source    string `json:"source,omitempty"`
}

func LoadDir(dir string) ([]Seed, error) {
	var out []Seed
	path := filepath.Join(dir, "seeds.jsonl")
	f, err := os.Open(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var s Seed
			if err := json.Unmarshal([]byte(line), &s); err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	fixSeeds, err := loadFixSeeds(dir)
	if err != nil {
		return nil, err
	}
	return append(out, fixSeeds...), nil
}

func loadFixSeeds(dir string) ([]Seed, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Seed
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		stem := strings.TrimSuffix(name, ".sql")
		sqlBytes, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		s := Seed{SQL: string(sqlBytes), Engine: "both", Source: name}
		metaBytes, err := os.ReadFile(filepath.Join(dir, stem+".meta.json"))
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err == nil {
			var meta struct {
				Engine    string `json:"engine"`
				SQLMode   uint64 `json:"sql_mode"`
				ExpectSig string `json:"expect_sig"`
			}
			if err := json.Unmarshal(metaBytes, &meta); err != nil {
				return nil, err
			}
			if meta.Engine != "" {
				s.Engine = meta.Engine
			}
			s.SQLMode = meta.SQLMode
			s.ExpectSig = meta.ExpectSig
		}
		out = append(out, s)
	}
	return out, nil
}

func WriteJSONL(path string, seeds []Seed) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	w := bufio.NewWriter(tmp)
	for _, s := range seeds {
		b, err := json.Marshal(s)
		if err != nil {
			tmp.Close()
			_ = os.Remove(name)
			return err
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			tmp.Close()
			_ = os.Remove(name)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

func Extract(flowMysqlDir string) ([]Seed, error) {
	files := []string{
		"ddl_parser_tidb_diff_test.go",
		"ddl_parser_test.go",
		"ddl_parser_alter_test.go",
		"ddl_parser_types_test.go",
		"ddl_parser_classify_test.go",
		"ddl_lexer_test.go",
	}
	var seeds []Seed
	expectBySQL := map[string]string{}
	seen := map[string]struct{}{}
	add := func(s Seed) {
		if s.SQL == "" {
			return
		}
		if s.Engine == "" {
			s.Engine = "both"
		}
		key := s.SQL + "\x00" + s.Engine + "\x00" + strconv.FormatUint(s.SQLMode, 10) + "\x00" + s.ExpectSig
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		seeds = append(seeds, s)
	}

	for _, name := range files {
		path := filepath.Join(flowMysqlDir, name)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, err
		}
		if name == "ddl_parser_tidb_diff_test.go" {
			collectExpectMaps(file, expectBySQL)
			collectTidbModeCases(file, name, add)
			collectTidbCorpus(file, name, expectBySQL, add)
		}
		collectFieldStrings(file, name, add)
		collectDDLStrings(file, name, add)
	}
	return seeds, nil
}

func collectExpectMaps(file *ast.File, out map[string]string) {
	ast.Inspect(file, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if name.Name != "tidbDiffOverrides" && name.Name != "tidbDiffFailWant" {
				continue
			}
			if i >= len(vs.Values) {
				continue
			}
			lit, ok := vs.Values[i].(*ast.CompositeLit)
			if !ok {
				continue
			}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				k, ok1 := evalString(kv.Key)
				v, ok2 := evalString(kv.Value)
				if ok1 && ok2 {
					out[k] = v
				}
			}
		}
		return true
	})
}

func collectTidbModeCases(file *ast.File, source string, add func(Seed)) {
	forEachNamedComposite(file, "tidbDiffModeCases", func(cl *ast.CompositeLit) {
		for _, elt := range cl.Elts {
			if child, ok := elt.(*ast.CompositeLit); ok {
				add(parseModeCase(child, source))
			}
		}
	})
	ast.Inspect(file, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok || typeName(cl.Type) != "tidbDiffModeCase" {
			return true
		}
		add(parseModeCase(cl, source))
		return true
	})
}

// collectTidbCorpus is the only collector that attaches the
// tidbDiffOverrides/tidbDiffFailWant annotations: those expectations were
// written for the corpus context (the entry's declared engine, sql_mode 0),
// so string-mined duplicates of the same SQL must not inherit them.
func collectTidbCorpus(file *ast.File, source string, expectBySQL map[string]string, add func(Seed)) {
	annotated := func(s Seed) Seed {
		if exp, ok := expectBySQL[s.SQL]; ok {
			s.ExpectSig = exp
		}
		return s
	}
	forEachNamedComposite(file, "tidbDiffCorpus", func(cl *ast.CompositeLit) {
		for _, elt := range cl.Elts {
			if child, ok := elt.(*ast.CompositeLit); ok {
				add(annotated(parseCorpusCase(child, source)))
			}
		}
	})
	ast.Inspect(file, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok || typeName(cl.Type) != "tidbDiffCase" {
			return true
		}
		add(annotated(parseCorpusCase(cl, source)))
		return true
	})
}

func parseModeCase(cl *ast.CompositeLit, source string) Seed {
	s := Seed{Engine: "mysql", Source: source}
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := keyName(kv.Key)
		if !ok {
			continue
		}
		switch key {
		case "sql":
			s.SQL, _ = evalString(kv.Value)
		case "want":
			s.ExpectSig, _ = evalString(kv.Value)
		case "sqlMode":
			s.SQLMode = evalMode(kv.Value)
		case "isMariaDB":
			if evalBool(kv.Value) {
				s.Engine = "mariadb"
			}
		}
	}
	return s
}

func parseCorpusCase(cl *ast.CompositeLit, source string) Seed {
	s := Seed{Engine: "both", Source: source}
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := keyName(kv.Key)
		if !ok {
			continue
		}
		switch key {
		case "sql":
			s.SQL, _ = evalString(kv.Value)
		case "engine":
			s.Engine, _ = evalString(kv.Value)
		}
	}
	return s
}

func forEachNamedComposite(file *ast.File, name string, fn func(*ast.CompositeLit)) {
	ast.Inspect(file, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, ident := range vs.Names {
			if ident.Name != name || i >= len(vs.Values) {
				continue
			}
			if cl, ok := vs.Values[i].(*ast.CompositeLit); ok {
				fn(cl)
			}
		}
		return true
	})
}

func collectFieldStrings(file *ast.File, source string, add func(Seed)) {
	fields := map[string]bool{"sql": true, "query": true, "input": true, "src": true}
	ast.Inspect(file, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := keyName(kv.Key)
		if !ok || !fields[key] {
			return true
		}
		if s, ok := evalString(kv.Value); ok {
			add(Seed{SQL: s, Engine: "both", Source: source})
		}
		return true
	})
}

func collectDDLStrings(file *ast.File, source string, add func(Seed)) {
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		s, ok := evalString(lit)
		if ok && looksLikeSQL(s) {
			add(Seed{SQL: s, Engine: "both", Source: source})
		}
		return true
	})
}

func evalString(expr ast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(e.Value)
		return s, err == nil
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		a, ok1 := evalString(e.X)
		b, ok2 := evalString(e.Y)
		return a + b, ok1 && ok2
	case *ast.ParenExpr:
		return evalString(e.X)
	default:
		v := constant.MakeFromLiteral(fmt.Sprint(expr), token.STRING, 0)
		if v.Kind() == constant.String {
			return constant.StringVal(v), true
		}
		return "", false
	}
}

func evalMode(expr ast.Expr) uint64 {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.INT {
			v, _ := strconv.ParseUint(e.Value, 0, 64)
			return v
		}
	case *ast.Ident:
		switch e.Name {
		case "sqlModeANSIQuotes":
			return sqlModeANSIQuotes
		case "sqlModeOracle":
			return sqlModeOracle
		case "sqlModeMSSQL":
			return sqlModeMSSQL
		case "sqlModeNoBackslashEscapes":
			return sqlModeNoBackslashEscapes
		}
	case *ast.BinaryExpr:
		if e.Op == token.OR {
			return evalMode(e.X) | evalMode(e.Y)
		}
	case *ast.ParenExpr:
		return evalMode(e.X)
	}
	return 0
}

func evalBool(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == "true"
}

func typeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.ArrayType:
		return typeName(e.Elt)
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return ""
}

func keyName(expr ast.Expr) (string, bool) {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return "", false
	}
	return id.Name, true
}

func looksLikeSQL(s string) bool {
	u := strings.ToUpper(strings.TrimSpace(s))
	heads := []string{"ALTER ", "RENAME ", "CREATE ", "DROP ", "SET STATEMENT ", "XA ", "COMMIT", "REPAIR ", "INSERT ", "UPDATE ", "DELETE "}
	for _, h := range heads {
		if strings.HasPrefix(u, h) || strings.Contains(u, " ALTER TABLE ") {
			return true
		}
	}
	return false
}
