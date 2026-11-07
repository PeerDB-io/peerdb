package testhooks

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type callSite struct {
	ident string
	fn    string
	pos   string
}

// flowRoot is the module root, two levels above this package.
const flowRoot = "../.."

func findCallSites(t *testing.T) []callSite {
	t.Helper()
	var calls []callSite
	fset := token.NewFileSet()
	err := filepath.WalkDir(flowRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "e2e" || d.Name() == "generated" || d.Name() == "testhooks" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !isPkg(fn.X, "testhooks") || (fn.Sel.Name != "Failpoint" && fn.Sel.Name != "Pausepoint") {
				return true
			}
			site, ok := call.Args[1].(*ast.SelectorExpr)
			if !ok || !isPkg(site.X, "testhooks") {
				t.Errorf("%s: site argument must be a testhooks.Site variable", fset.Position(call.Pos()))
				return true
			}
			rel, _ := filepath.Rel(flowRoot, path)
			calls = append(calls, callSite{
				ident: site.Sel.Name,
				fn:    fn.Sel.Name,
				pos:   fmt.Sprintf("flow/%s:%d", filepath.ToSlash(rel), fset.Position(call.Pos()).Line),
			})
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return calls
}

func isPkg(x ast.Expr, name string) bool {
	id, ok := x.(*ast.Ident)
	return ok && id.Name == name
}

// declaredSites maps each Site variable in sites.go to its Name field.
func declaredSites(t *testing.T) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "sites.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, ident := range vs.Names {
				lit, ok := vs.Values[i].(*ast.CompositeLit)
				if !ok || !isPkg(lit.Type, "Site") {
					continue
				}
				for _, elt := range lit.Elts {
					kv := elt.(*ast.KeyValueExpr)
					if isPkg(kv.Key, "Name") {
						name, err := strconv.Unquote(kv.Value.(*ast.BasicLit).Value)
						if err != nil {
							t.Fatal(err)
						}
						out[ident.Name] = name
					}
				}
			}
		}
	}
	return out
}

// TestSitesMatchCallSites checks that every declared site is listed in Sites,
// is called exactly once, and is called with the function matching its kind.
// Set TESTHOOKS_SITE_TABLE to a path to also write the site table as markdown.
func TestSitesMatchCallSites(t *testing.T) {
	declared := declaredSites(t)
	byName := map[string]Site{}
	for _, s := range Sites {
		if _, dup := byName[s.Name]; dup {
			t.Errorf("duplicate site name %s", s.Name)
		}
		byName[s.Name] = s
	}
	for ident, name := range declared {
		if _, ok := byName[name]; !ok {
			t.Errorf("%s (%s) is declared but missing from Sites", ident, name)
		}
	}
	if len(declared) != len(Sites) {
		t.Errorf("%d declared sites, %d in Sites", len(declared), len(Sites))
	}

	callsByName := map[string][]callSite{}
	for _, c := range findCallSites(t) {
		name, ok := declared[c.ident]
		if !ok {
			t.Errorf("%s: testhooks.%s is not a declared site", c.pos, c.ident)
			continue
		}
		site := byName[name]
		if (site.Kind == KindPause) != (c.fn == "Pausepoint") {
			t.Errorf("%s: %s is a %s site but is called with %s", c.pos, name, site.Kind, c.fn)
		}
		callsByName[name] = append(callsByName[name], c)
	}
	for _, s := range Sites {
		if n := len(callsByName[s.Name]); n != 1 {
			t.Errorf("%s has %d call sites, want 1", s.Name, n)
		}
	}

	if path := os.Getenv("TESTHOOKS_SITE_TABLE"); path != "" {
		//nolint:gosec // Developer-chosen output path for a test report.
		if err := os.WriteFile(path, []byte(siteTable(callsByName)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func siteTable(callsByName map[string][]callSite) string {
	var b strings.Builder
	b.WriteString("| Site | Kind | Loop | Workloads | Schema change only | Location |\n|---|---|---|---|---|---|\n")
	for _, s := range Sites {
		var workloads []string
		for _, w := range Workloads {
			if s.In(w) {
				workloads = append(workloads, w.String())
			}
		}
		loc := ""
		if calls := callsByName[s.Name]; len(calls) > 0 {
			loc = "`" + calls[0].pos + "`"
		}
		schemaOnly := ""
		if s.SchemaChangeOnly {
			schemaOnly = "yes"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s |\n",
			s.Name, s.Kind, s.Loop, strings.Join(workloads, ", "), schemaOnly, loc)
	}

	counts := map[string]int{}
	for _, c := range GenerateCases() {
		counts[c.Workload.String()+"\t"+string(c.Kind)]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteString("\n| Workload | Case kind | Cases |\n|---|---|---|\n")
	total := 0
	for _, k := range keys {
		w, kind, _ := strings.Cut(k, "\t")
		fmt.Fprintf(&b, "| %s | %s | %d |\n", w, kind, counts[k])
		total += counts[k]
	}
	fmt.Fprintf(&b, "| **total** | | **%d** |\n", total)
	return b.String()
}

func TestGenerateCasesAreUnique(t *testing.T) {
	cases := GenerateCases()
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.Name()] {
			t.Errorf("duplicate case %s", c.Name())
		}
		seen[c.Name()] = true
		if c.Kind == CasePauseFail && (c.First.Kind != KindPause || c.Second.Kind != KindFail) {
			t.Errorf("%s: pause_fail needs a pause site then a fail site", c.Name())
		}
		if c.Kind == CaseRetryPair && (c.First.Kind != KindFail || c.Second.Kind != KindFail) {
			t.Errorf("%s: retry_pair needs two fail sites", c.Name())
		}
	}
}

func TestShardCasesRunEveryCaseOnce(t *testing.T) {
	want := map[string]bool{}
	for _, c := range GenerateCases() {
		want[c.Name()] = true
	}
	got := map[string]int{}
	for _, w := range Workloads {
		minSize, maxSize := len(want), 0
		for i := range Shards {
			shard := ShardCases(w, i)
			minSize, maxSize = min(minSize, len(shard)), max(maxSize, len(shard))
			for _, c := range shard {
				if c.Workload != w {
					t.Errorf("shard %d of %s holds %s", i, w, c.Name())
				}
				got[c.Name()]++
			}
		}
		if maxSize-minSize > 1 {
			t.Errorf("%s shards hold between %d and %d cases, want an even split", w, minSize, maxSize)
		}
	}
	for name := range want {
		if got[name] != 1 {
			t.Errorf("%s runs in %d shards, want 1", name, got[name])
		}
	}
	if len(got) != len(want) {
		t.Errorf("shards hold %d distinct cases, want %d", len(got), len(want))
	}
}
