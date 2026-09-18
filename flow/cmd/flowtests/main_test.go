package main

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func fixtures() (map[string]group, []testPackage, []string) {
	groups := map[string]group{
		"postgres": {Packages: []string{"connectors/postgres", "connectors/utils"}, E2E: []string{"TestPG"}},
		"mysql":    {Packages: []string{"connectors/mysql", "pkg/mysql"}, E2E: []string{"TestMySQL"}},
	}
	packages := []testPackage{
		{Path: "connectors/postgres", ImportPath: "flow/connectors/postgres", Module: "."},
		{Path: "connectors/utils", ImportPath: "flow/connectors/utils", Module: "."},
		{Path: "connectors/utils/structured", ImportPath: "flow/connectors/utils/structured", Module: "."},
		{Path: "connectors/mysql", ImportPath: "flow/connectors/mysql", Module: "."},
		{Path: "connectors/mysqlnew", ImportPath: "flow/connectors/mysqlnew", Module: "."},
		{Path: "internal", ImportPath: "flow/internal", Module: "."},
		{Path: "e2e", ImportPath: "flow/e2e", Module: "."},
		{Path: "pkg/mysql", ImportPath: "flow/pkg/mysql", Module: "pkg"},
		{Path: "pkg/common", ImportPath: "flow/pkg/common", Module: "pkg"},
	}
	return groups, packages, []string{"TestPG", "TestMySQL"}
}

func TestPackagesPartitionAcrossJobs(t *testing.T) {
	groups, packages, tests := fixtures()
	seen := make(map[string]string)
	for _, name := range []string{"postgres", "mysql", "unit"} {
		plan, err := selectTests(groups, packages, tests, name)
		require.NoError(t, err)
		for _, pkg := range append(slices.Clone(plan.FlowPackages), plan.PkgPackages...) {
			require.NotContains(t, seen, pkg, "package selected more than once: %s", name)
			seen[pkg] = name
		}
		if name == "unit" {
			require.Empty(t, plan.E2ERunPattern, "unit must not run E2E")
			require.Equal(t, []string{"flow/pkg/common"}, plan.PkgPackages, "shared pkg utilities must default to unit")
		} else {
			run := regexp.MustCompile(plan.E2ERunPattern)
			for _, test := range tests {
				owned := slices.Contains(groups[name].E2E, test)
				require.Equal(t, owned, run.MatchString(test), "incorrect E2E selection for %s in %s", test, name)
				require.NotRegexp(t, run, test+"Extra", "E2E selection must match whole names")
			}
		}
	}
	require.Len(t, seen, len(packages)-1, "some packages omitted")
	require.Equal(t, "postgres", seen["flow/connectors/utils/structured"], "package ownership must include descendants")
	require.Equal(t, "unit", seen["flow/connectors/mysqlnew"],
		"new packages must default to unit, without matching partial path components")
}

func TestInvalidOwnership(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(map[string]group, *[]string)
		want   string
	}{
		{"unregistered suite", func(_ map[string]group, tests *[]string) {
			*tests = append(*tests, "TestNewSuite")
		}, "unassigned E2E test TestNewSuite"},
		{"removed suite", func(groups map[string]group, _ *[]string) {
			groups["new"] = group{E2E: []string{"TestRemoved"}}
		}, "registered E2E test TestRemoved does not exist"},
		{"duplicate suite", func(groups map[string]group, _ *[]string) {
			groups["new"] = group{E2E: []string{"TestPG"}}
		}, "E2E test TestPG assigned more than once"},
		{"overlapping packages", func(groups map[string]group, _ *[]string) {
			groups["new"] = group{Packages: []string{"connectors/utils/structured"}}
		}, "package connectors/utils/structured assigned more than once"},
		{"stale package", func(groups map[string]group, _ *[]string) {
			groups["new"] = group{Packages: []string{"pkg/removed"}}
		}, "matches no packages"},
		{"E2E package assignment", func(groups map[string]group, _ *[]string) {
			groups["new"] = group{Packages: []string{"e2e"}}
		}, "invalid package prefix"},
		{"unit registration", func(groups map[string]group, _ *[]string) {
			groups["unit"] = group{}
		}, "unit is the automatic catch-all"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups, packages, tests := fixtures()
			tc.modify(groups, &tests)
			// Validation also runs in the unit job, which executes no E2E tests.
			_, err := selectTests(groups, packages, tests, "unit")
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDiscoverEntryPoints(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOWORK", "off")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/discovery\ngo 1.27.0\n"), 0o600))
	const source = `package e2e
import (
    "fmt"
    "os"
    "testing"
)
func init() { fmt.Println("initialization log") }
// func TestComment(t *testing.T) {}
func TestMain(m *testing.M) { os.Exit(m.Run()) }
func TestPG(t *testing.T) { panic("listing must not execute tests") }
func Test_Underscore(t *testing.T) {}
type Suite struct{}
func (s Suite) TestMethod() {}
func FuzzRows(f *testing.F) { panic("listing must not execute fuzz seeds") }
func BenchmarkRows(b *testing.B) { panic("listing must not execute benchmarks") }
func Example() {
    fmt.Println("hello")
    // Output: hello
}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "suite_test.go"), []byte(source), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "external_test.go"), []byte(`package e2e_test
import "testing"
func TestExternal(t *testing.T) {}
`), 0o600))
	tests, err := discoverTests(dir)
	require.NoError(t, err)
	want := []string{"TestExternal", "TestPG", "Test_Underscore"}
	require.Equal(t, want, tests)
	// A package that cannot compile must fail discovery, not look unassigned.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken_test.go"), []byte("package e2e\nvar _ = undefined\n"), 0o600))
	_, err = discoverTests(dir)
	require.Error(t, err, "discovery accepted a package with a compilation error")
}

func TestEmptyAndUnknownGroups(t *testing.T) {
	groups, packages, tests := fixtures()
	_, err := selectTests(groups, packages, tests, "typo")
	require.Error(t, err, "unknown group must fail")
	groups["empty"] = group{}
	plan, err := selectTests(groups, packages, tests, "empty")
	require.NoError(t, err)
	require.NotRegexp(t, plan.E2ERunPattern, "TestPG", "an empty group must not select every E2E test")
}
