package connmysql

// This file enforces local MySQL connector integration test conventions so the
// tests remain discoverable by CI/Tilt selectors and by IDE CodeLens test
// runners, which need inline literal subtest names:
//   - Tests that run against both local MySQL and MariaDB must be named
//     TestIntegration... and must declare inline subtests named "mysql" and
//     "mariadb" using a []struct{ name string } table.
//   - Tests that intentionally run only against local MySQL must be named
//     TestMySQLOnlyIntegration...
//   - Tests that intentionally run only against local MariaDB must be named
//     TestMariaOnlyIntegration...
//   - Other Test... functions in this package are treated as unit tests and
//     must not reach local MySQL/MariaDB test configuration helpers.
//
// This check uses syntax only, not type resolution. Source names need to stay as
// string literals so simple IDE and AST tooling can find them, and helper
// indirection must remain visible through same-package function calls.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

//nolint:lll
const (
	missingSourceSubtestsMessage   = "TestIntegration tests must declare inline mysql and mariadb source subtests"
	misnamedIntegrationTestMessage = "MySQL integration test must be named TestIntegration.../{mysql,mariadb}, TestMySQLOnlyIntegration..., or TestMariaOnlyIntegration..."
)

var mysqlDBConfigHelpers = map[string]struct{}{
	"GetMySQLConfigFromEnv":     {},
	"GetMariaDBConfigFromEnv":   {},
	"MySQLTestHost":             {},
	"MySQLTestHostWithFallback": {},
	"MariaDBTestHost":           {},
	"MySQLSSHUpstreamHost":      {},
}

var mysqlDBHostEnvVars = map[string]struct{}{
	"CI_MYSQL_HOST":     {},
	"CI_MARIADB_HOST":   {},
	"CI_SSH_MYSQL_HOST": {},
}

type mysqlTestFunc struct {
	name            string
	decl            *ast.FuncDecl
	reachesDBConfig bool
	calls           map[string]struct{}
}

type mysqlConventionFailure struct {
	testName string
	message  string
}

func (f mysqlConventionFailure) String() string {
	return f.testName + ": " + f.message
}

func TestMySQLIntegrationTestNamingConvention(t *testing.T) {
	t.Parallel()

	failures, err := mysqlIntegrationConventionFailuresFromDir(".")
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) > 0 {
		t.Fatalf("MySQL integration test convention violations:\n%s",
			strings.Join(formatMySQLConventionFailures(failures), "\n"))
	}
}

func TestMySQLIntegrationTestNamingConventionAnalyzer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		files        map[string]string
		wantFailures []string
	}{
		{
			name: "shared integration accepts inline mysql and mariadb subtests",
			files: map[string]string{
				"valid_test.go": `
package connmysql

func TestIntegrationGood(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
		{name: "mariadb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newTestConnector(t, nil, tc.name)
		})
	}
}

func newTestConnector(args ...any) {
	internal.GetMySQLConfigFromEnv(nil, nil)
}
`,
			},
		},
		{
			name: "mysql only integration accepts transitive db config helper",
			files: map[string]string{
				"valid_test.go": `
package connmysql

func TestMySQLOnlyIntegrationSSH(t *testing.T) {
	resolveMySQL()
}

func resolveMySQL() {
	internal.MySQLSSHUpstreamHost()
}
`,
			},
		},
		{
			name: "maria only integration accepts transitive db config helper",
			files: map[string]string{
				"valid_test.go": `
package connmysql

func TestMariaOnlyIntegrationGTID(t *testing.T) {
	resolveMariaDB()
}

func resolveMariaDB() {
	internal.MariaDBTestHost()
}
`,
			},
		},
		{
			name: "shared integration allows inner table tests",
			files: map[string]string{
				"valid_test.go": `
package connmysql

func TestIntegrationWithInnerTable(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
		{name: "mariadb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, inner := range []struct {
				name string
				sql  string
			}{
				{name: "quoted", sql: "select 1"},
				{name: "plain", sql: "select 2"},
			} {
				t.Run(inner.name, func(t *testing.T) {
					newTestConnector(t, nil, tc.name)
				})
			}
		})
	}
}

func newTestConnector(args ...any) {
	internal.GetMySQLConfigFromEnv(nil, nil)
}
`,
			},
		},
		{
			name: "db reaching mysql test must be classified by name",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestForgotClassification(t *testing.T) {
	newTestConnector()
}

func newTestConnector() {
	internal.GetMySQLConfigFromEnv(nil, nil)
}
`,
			},
			wantFailures: []string{
				"TestForgotClassification: MySQL integration test must be named TestIntegration.../",
			},
		},
		{
			name: "db config helper detection propagates across files",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestForgotClassification(t *testing.T) {
	newTestConnector()
}
`,
				"helpers_test.go": `
package connmysql

func newTestConnector() {
	internal.GetMariaDBConfigFromEnv(nil, nil)
}
`,
			},
			wantFailures: []string{
				"TestForgotClassification: MySQL integration test must be named TestIntegration.../",
			},
		},
		{
			name: "integration test must include mariadb subtest",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestIntegrationMissingMariaDB(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
	} {
		t.Run(tc.name, func(t *testing.T) {})
	}
}
`,
			},
			wantFailures: []string{
				"TestIntegrationMissingMariaDB: TestIntegration tests must declare inline mysql and mariadb source subtests",
			},
		},
		{
			name: "integration test source table must be inline",
			files: map[string]string{
				"bad_test.go": `
package connmysql

var sources = []struct {
	name string
}{
	{name: "mysql"},
	{name: "mariadb"},
}

func TestIntegrationIndirectSources(t *testing.T) {
	for _, tc := range sources {
		t.Run(tc.name, func(t *testing.T) {})
	}
}
`,
			},
			wantFailures: []string{
				"TestIntegrationIndirectSources: TestIntegration tests must declare inline mysql and mariadb source subtests",
			},
		},
		{
			name: "integration test source names must be string literals",
			files: map[string]string{
				"bad_test.go": `
package connmysql

const (
	mysqlSource   = "mysql"
	mariadbSource = "mariadb"
)

func TestIntegrationConstantSources(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: mysqlSource},
		{name: mariadbSource},
	} {
		t.Run(tc.name, func(t *testing.T) {})
	}
}
`,
			},
			wantFailures: []string{
				"TestIntegrationConstantSources: TestIntegration tests must declare inline mysql and mariadb source subtests",
			},
		},
		{
			name: "unit test without db config access is ignored",
			files: map[string]string{
				"valid_test.go": `
package connmysql

func TestParseOnly(t *testing.T) {}
`,
			},
		},
		{
			name: "direct os env host lookup marks test as db reaching",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestForgotClassification(t *testing.T) {
	os.Getenv("CI_MYSQL_HOST")
}
`,
			},
			wantFailures: []string{
				"TestForgotClassification: MySQL integration test must be named TestIntegration.../",
			},
		},
		{
			name: "direct os env host lookupenv marks test as db reaching",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestForgotClassification(t *testing.T) {
	os.LookupEnv("CI_MARIADB_HOST")
}
`,
			},
			wantFailures: []string{
				"TestForgotClassification: MySQL integration test must be named TestIntegration.../",
			},
		},
		{
			name: "internal host helpers mark tests as db reaching",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestUsesMariaDBTestHost(t *testing.T) {
	internal.MariaDBTestHost()
}

func TestUsesMySQLTestHost(t *testing.T) {
	internal.MySQLTestHost()
}

func TestUsesMySQLTestHostWithFallback(t *testing.T) {
	internal.MySQLTestHostWithFallback("mysql")
}
`,
			},
			wantFailures: []string{
				"TestUsesMariaDBTestHost: MySQL integration test must be named TestIntegration.../",
				"TestUsesMySQLTestHost: MySQL integration test must be named TestIntegration.../",
				"TestUsesMySQLTestHostWithFallback: MySQL integration test must be named TestIntegration.../",
			},
		},
		{
			name: "mixed violations in one file are reported in stable test name order",
			files: map[string]string{
				"bad_test.go": `
package connmysql

func TestForgotClassification(t *testing.T) {
	internal.MySQLTestHost()
}

func TestIntegrationMissingMariaDB(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
	} {
		t.Run(tc.name, func(t *testing.T) {})
	}
}
`,
			},
			wantFailures: []string{
				"TestForgotClassification: MySQL integration test must be named TestIntegration.../",
				"TestIntegrationMissingMariaDB: TestIntegration tests must declare inline mysql and mariadb source subtests",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failures, err := mysqlIntegrationConventionFailuresFromSources(tt.files)
			if err != nil {
				t.Fatal(err)
			}
			gotFailures := formatMySQLConventionFailures(failures)
			if len(gotFailures) != len(tt.wantFailures) {
				t.Fatalf("failure count mismatch\ngot:\n%s\nwant prefixes:\n%s",
					strings.Join(gotFailures, "\n"), strings.Join(tt.wantFailures, "\n"))
			}
			for i, want := range tt.wantFailures {
				if !strings.HasPrefix(gotFailures[i], want) {
					t.Fatalf("failure %d mismatch\ngot:         %s\nwant prefix: %s", i, gotFailures[i], want)
				}
			}
		})
	}
}

func mysqlIntegrationConventionFailuresFromDir(dir string) ([]mysqlConventionFailure, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	sources := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		sources[path] = string(source)
	}
	return mysqlIntegrationConventionFailuresFromSources(sources)
}

func mysqlIntegrationConventionFailuresFromSources(sources map[string]string) ([]mysqlConventionFailure, error) {
	fset := token.NewFileSet()
	funcs, err := parseMySQLTestFuncs(fset, sources)
	if err != nil {
		return nil, err
	}
	return mysqlIntegrationConventionFailures(funcs), nil
}

func mysqlIntegrationConventionFailures(funcs map[string]*mysqlTestFunc) []mysqlConventionFailure {
	reachingFuncs := dbConfigReachingFuncs(funcs)
	var failures []mysqlConventionFailure

	for _, name := range sortedFuncNames(funcs) {
		fn := funcs[name]
		if !strings.HasPrefix(name, "Test") {
			continue
		}

		switch {
		case strings.HasPrefix(name, "TestIntegration"):
			if !hasInlineMySQLAndMariaDBSourceSubtests(fn.decl) {
				failures = append(failures, mysqlConventionFailure{
					testName: name,
					message:  missingSourceSubtestsMessage,
				})
			}
		case reachingFuncs[name] &&
			!strings.HasPrefix(name, "TestMySQLOnlyIntegration") &&
			!strings.HasPrefix(name, "TestMariaOnlyIntegration"):
			failures = append(failures, mysqlConventionFailure{
				testName: name,
				message:  misnamedIntegrationTestMessage,
			})
		}
	}

	return failures
}

func parseMySQLTestFuncs(fset *token.FileSet, sources map[string]string) (map[string]*mysqlTestFunc, error) {
	funcs := make(map[string]*mysqlTestFunc)
	for _, filename := range sortedSourceNames(sources) {
		file, err := parser.ParseFile(fset, filename, sources[filename], 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", filename, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Body == nil {
				continue
			}
			funcs[fn.Name.Name] = collectMySQLTestFunc(fn)
		}
	}
	return funcs, nil
}

func collectMySQLTestFunc(fn *ast.FuncDecl) *mysqlTestFunc {
	info := &mysqlTestFunc{
		name:  fn.Name.Name,
		decl:  fn,
		calls: make(map[string]struct{}),
	}

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		switch callee := call.Fun.(type) {
		case *ast.Ident:
			info.calls[callee.Name] = struct{}{}
		case *ast.SelectorExpr:
			if isDBConfigCall(callee, call) {
				info.reachesDBConfig = true
			}
		}
		return true
	})

	return info
}

func isDBConfigCall(callee *ast.SelectorExpr, call *ast.CallExpr) bool {
	pkg, ok := callee.X.(*ast.Ident)
	if !ok {
		return false
	}

	if pkg.Name == "internal" {
		if _, ok := mysqlDBConfigHelpers[callee.Sel.Name]; ok {
			return true
		}
		return callee.Sel.Name == "GetEnvString" && firstStringArgIn(call, mysqlDBHostEnvVars)
	}

	return pkg.Name == "os" &&
		(callee.Sel.Name == "Getenv" || callee.Sel.Name == "LookupEnv") &&
		firstStringArgIn(call, mysqlDBHostEnvVars)
}

func firstStringArgIn(call *ast.CallExpr, values map[string]struct{}) bool {
	if len(call.Args) == 0 {
		return false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return false
	}
	_, ok = values[value]
	return ok
}

func dbConfigReachingFuncs(funcs map[string]*mysqlTestFunc) map[string]bool {
	reaches := make(map[string]bool, len(funcs))
	for name, fn := range funcs {
		reaches[name] = fn.reachesDBConfig
	}

	for changed := true; changed; {
		changed = false
		for name, fn := range funcs {
			if reaches[name] {
				continue
			}
			for called := range fn.calls {
				if reaches[called] {
					reaches[name] = true
					changed = true
					break
				}
			}
		}
	}

	return reaches
}

func hasInlineMySQLAndMariaDBSourceSubtests(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if found {
			return false
		}

		stmt, ok := n.(*ast.RangeStmt)
		if !ok {
			return true
		}
		value, ok := stmt.Value.(*ast.Ident)
		if !ok || value.Name == "_" {
			return true
		}

		sources, ok := inlineNameSourceTable(stmt.X)
		if !ok || !runsSubtestWithNameField(stmt.Body, value.Name) {
			return true
		}
		found = sources["mysql"] && sources["mariadb"]
		return !found
	})
	return found
}

func inlineNameSourceTable(expr ast.Expr) (map[string]bool, bool) {
	table, ok := expr.(*ast.CompositeLit)
	if !ok || !isInlineStructSliceWithNameString(table.Type) {
		return nil, false
	}

	sources := make(map[string]bool)
	for _, elt := range table.Elts {
		row, ok := elt.(*ast.CompositeLit)
		if !ok {
			continue
		}
		for _, rowElt := range row.Elts {
			kv, ok := rowElt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "name" {
				continue
			}
			value, ok := stringLiteralValue(kv.Value)
			if ok {
				sources[value] = true
			}
		}
	}
	return sources, len(sources) > 0
}

func isInlineStructSliceWithNameString(expr ast.Expr) bool {
	arrayType, ok := expr.(*ast.ArrayType)
	if !ok {
		return false
	}
	structType, ok := arrayType.Elt.(*ast.StructType)
	if !ok {
		return false
	}

	for _, field := range structType.Fields.List {
		ident, ok := field.Type.(*ast.Ident)
		if !ok || ident.Name != "string" {
			continue
		}
		for _, name := range field.Names {
			if name.Name == "name" {
				return true
			}
		}
	}
	return false
}

func runsSubtestWithNameField(body *ast.BlockStmt, rangeValue string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		run, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || run.Sel.Name != "Run" {
			return true
		}
		receiver, ok := run.X.(*ast.Ident)
		if !ok || receiver.Name != "t" {
			return true
		}
		name, ok := call.Args[0].(*ast.SelectorExpr)
		if !ok || name.Sel.Name != "name" {
			return true
		}
		value, ok := name.X.(*ast.Ident)
		found = ok && value.Name == rangeValue
		return !found
	})
	return found
}

func stringLiteralValue(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

func formatMySQLConventionFailures(failures []mysqlConventionFailure) []string {
	formatted := make([]string, 0, len(failures))
	for _, failure := range failures {
		formatted = append(formatted, failure.String())
	}
	return formatted
}

func sortedFuncNames(funcs map[string]*mysqlTestFunc) []string {
	names := make([]string, 0, len(funcs))
	for name := range funcs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedSourceNames(sources map[string]string) []string {
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
