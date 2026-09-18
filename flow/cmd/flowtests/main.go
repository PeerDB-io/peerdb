// Command flowtests validates test ownership and runs a source or unit job.
// Run from flow/: go run ./cmd/flowtests -group mysql [-list]
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

func main() {
	groupName := flag.String("group", "unit", "source group from test-jobs.yml, or unit")
	listOnly := flag.Bool("list", false, "validate ownership and print the selection without running tests")
	flag.Parse()
	if err := run(*groupName, *listOnly); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(groupName string, listOnly bool) error {
	data, err := os.ReadFile("test-jobs.yml")
	if err != nil {
		return err
	}
	groups, err := decodeGroups(data)
	if err != nil {
		return err
	}
	var packages []testPackage
	for _, module := range []string{".", "pkg"} {
		pkgs, err := listPackages(module)
		if err != nil {
			return err
		}
		packages = append(packages, pkgs...)
	}
	var e2eTests []string
	for _, pkg := range packages {
		if pkg.Path == "e2e" {
			e2eTests, err = discoverTests(pkg.Dir)
			if err != nil {
				return err
			}
		}
	}
	if len(e2eTests) == 0 {
		return errors.New("no E2E entry points discovered")
	}
	selection, err := selectTests(groups, packages, e2eTests, groupName)
	if err != nil {
		return fmt.Errorf("test-jobs.yml: %w", err)
	}
	if listOnly {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(selection)
	}
	return executePlan(selection, runSettings{
		logDir:      os.Getenv("TEST_LOG_DIR"),
		coverageDir: os.Getenv("COVERAGE_DIR"),
	}, (*exec.Cmd).Run)
}

type group struct {
	Packages []string `yaml:"packages"`
	E2E      []string `yaml:"e2e"`
}

func decodeGroups(data []byte) (map[string]group, error) {
	var groups map[string]group
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&groups); err != nil {
		return nil, fmt.Errorf("test-jobs.yml: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("test-jobs.yml: expected a single YAML document")
	}
	return groups, nil
}

type testPackage struct {
	Dir        string `json:"Dir"`
	ImportPath string `json:"ImportPath"`
	// Path is relative to flow, including pkg/ for the nested module.
	Path   string `json:"-"`
	Module string `json:"-"`
}

func listPackages(module string) ([]testPackage, error) {
	// -find discovers packages without building them or loading their dependencies.
	// Both modules must be visited explicitly.
	cmd := exec.Command("go", "list", "-find", "-json", "./...")
	cmd.Dir = module
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list packages in %s: %w", module, err)
	}
	root, err := filepath.Abs(".")
	if err != nil {
		return nil, err
	}
	var packages []testPackage
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg testPackage
		if err := decoder.Decode(&pkg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		rel, err := filepath.Rel(root, pkg.Dir)
		if err != nil {
			return nil, err
		}
		pkg.Path = filepath.ToSlash(rel)
		pkg.Module = module
		packages = append(packages, pkg)
	}
	return packages, nil
}

func discoverTests(dir string) ([]string, error) {
	// Let Go discover runnable tests. Listing compiles the package
	// and executes initialization/TestMain, but does not run test bodies.
	cmd := exec.Command("go", "test", "-list", ".", ".")
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list E2E tests: %w\n%s", err, output)
	}
	// Go prints one name per line, followed by a package summary. Ignore the
	// summary and ordinary initialization logs, which are not test names.
	namePattern := regexp.MustCompile(`^Test\S*$`)
	var tests []string
	for line := range strings.Lines(string(output)) {
		name := strings.TrimSpace(line)
		if namePattern.MatchString(name) {
			tests = append(tests, name)
		}
	}
	slices.Sort(tests)
	return tests, nil
}

func within(pkg, prefix string) bool {
	return pkg == prefix || strings.HasPrefix(pkg, prefix+"/")
}

type plan struct {
	E2ERunPattern string   `json:"e2e_run_pattern"`
	FlowPackages  []string `json:"flow_packages"`
	PkgPackages   []string `json:"pkg_packages"`
}

func selectTests(groups map[string]group, packages []testPackage, e2eTests []string, selected string) (plan, error) {
	result := plan{FlowPackages: []string{}, PkgPackages: []string{}}
	if _, ok := groups[selected]; selected != "unit" && !ok {
		return result, fmt.Errorf("unknown test group %q", selected)
	}
	packageOwners := make(map[string]string)
	testOwners := make(map[string]string)
	for name, group := range groups {
		if name == "unit" {
			return result, errors.New("unit is the automatic catch-all; do not register it")
		}
		for _, prefix := range group.Packages {
			if prefix == "." || path.Clean(prefix) != prefix || strings.HasPrefix(prefix, "/") ||
				strings.HasPrefix(prefix, "../") || within(prefix, "e2e") {
				return result, fmt.Errorf("invalid package prefix %q", prefix)
			}
			matched := false
			for _, pkg := range packages {
				if !within(pkg.Path, prefix) {
					continue
				}
				matched = true
				if owner, ok := packageOwners[pkg.Path]; ok {
					return result, fmt.Errorf("package %s assigned more than once (%s, %s)", pkg.Path, owner, name)
				}
				packageOwners[pkg.Path] = name
			}
			if !matched {
				return result, fmt.Errorf("package prefix %q in %s matches no packages", prefix, name)
			}
		}
		for _, test := range group.E2E {
			if owner, ok := testOwners[test]; ok {
				return result, fmt.Errorf("E2E test %s assigned more than once (%s, %s)", test, owner, name)
			}
			if !slices.Contains(e2eTests, test) {
				return result, fmt.Errorf("registered E2E test %s does not exist", test)
			}
			testOwners[test] = name
		}
	}
	for _, test := range e2eTests {
		if _, ok := testOwners[test]; !ok {
			return result, fmt.Errorf("unassigned E2E test %s: register it under a source", test)
		}
	}
	for _, pkg := range packages {
		if pkg.Path == "e2e" {
			continue
		}
		owner, ok := packageOwners[pkg.Path]
		if !ok {
			owner = "unit"
		}
		if owner != selected {
			continue
		}
		if pkg.Module == "pkg" {
			result.PkgPackages = append(result.PkgPackages, pkg.ImportPath)
		} else {
			result.FlowPackages = append(result.FlowPackages, pkg.ImportPath)
		}
	}
	slices.Sort(result.FlowPackages)
	slices.Sort(result.PkgPackages)
	if selected != "unit" {
		var run []string
		for _, test := range e2eTests {
			if testOwners[test] == selected {
				run = append(run, regexp.QuoteMeta(test))
			}
		}
		result.E2ERunPattern = testPattern(run)
	}
	return result, nil
}

func testPattern(names []string) string {
	if len(names) == 0 {
		return "^$"
	}
	return "^(" + strings.Join(names, "|") + ")$"
}
