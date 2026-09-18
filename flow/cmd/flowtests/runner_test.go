package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecutePlanRunsConcurrentlyAndWaitsAfterFailure(t *testing.T) {
	groups, packages, tests := fixtures()
	selection, err := selectTests(groups, packages, tests, "mysql")
	require.NoError(t, err)
	settings := runSettings{
		logDir:      filepath.Join(t.TempDir(), "logs with spaces"),
		coverageDir: filepath.Join(t.TempDir(), "coverage with spaces"),
	}
	failed := errors.New("test failed")
	started := make(chan *exec.Cmd, 3)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- executePlan(selection, settings, func(cmd *exec.Cmd) error {
			started <- cmd
			if err := writeTestReport(cmd); err != nil {
				return err
			}
			if cmd.Dir == "." && !slices.Contains(cmd.Args, "-run") {
				return failed
			}
			<-release
			return nil
		})
	}()
	calls := make(map[string]*exec.Cmd)
	for range 3 {
		select {
		case cmd := <-started:
			reportIndex := slices.Index(cmd.Args, "--junitfile")
			require.GreaterOrEqual(t, reportIndex, 0)
			require.Less(t, reportIndex+1, len(cmd.Args))
			calls[filepath.Base(cmd.Args[reportIndex+1])] = cmd
		case <-time.After(10 * time.Second):
			t.Fatal("all invocations must start before the blocked ones finish")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("runner returned before all invocations finished: %v", err)
	default:
	}
	data, err := os.ReadFile(filepath.Join(settings.logDir, "test-selection.json"))
	require.NoError(t, err)
	var recorded executionPlan
	require.NoError(t, json.Unmarshal(data, &recorded))
	require.Equal(t, selection, recorded.Selection)
	require.Equal(t, []string{"test-results.xml", "pkg-test-results.xml", "e2e-test-results.xml"}, recorded.Reports)
	unblock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, failed)
		require.ErrorContains(t, err, "test-results.xml")
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not finish after all invocations were released")
	}
	normalized, err := os.ReadFile(filepath.Join(settings.logDir, "normalized-test-results.ndjson"))
	require.NoError(t, err, "reports must be normalized even when tests fail")
	require.Empty(t, normalized, "empty reports produce empty NDJSON")
	require.Len(t, calls, 3)
	for report, cmd := range calls {
		if report != "e2e-test-results.xml" {
			require.NotContains(t, cmd.Args, "-run", "unexpected test filter in %s", report)
		}
		for _, arg := range []string{"-cover", "-test.gocoverdir=" + settings.coverageDir} {
			require.Contains(t, cmd.Args, arg, "report %s", report)
		}
	}
	pkg := calls["pkg-test-results.xml"]
	require.Equal(t, "pkg", pkg.Dir)
	require.Contains(t, pkg.Args, "github.com/PeerDB-io/peerdb/flow/pkg/...")
	e2e := calls["e2e-test-results.xml"]
	runIndex := slices.Index(e2e.Args, "-run")
	require.GreaterOrEqual(t, runIndex, 0, "E2E ownership filter missing")
	require.Less(t, runIndex+1, len(e2e.Args))
	require.Equal(t, selection.E2ERunPattern, e2e.Args[runIndex+1])
	require.Contains(t, e2e.Args, "./e2e")
}

func TestExecuteUnitWithoutCoverageOrE2E(t *testing.T) {
	groups, packages, tests := fixtures()
	selection, err := selectTests(groups, packages, tests, "unit")
	require.NoError(t, err)
	calls := make(chan *exec.Cmd, 3)
	err = executePlan(selection, runSettings{logDir: t.TempDir()}, func(cmd *exec.Cmd) error {
		calls <- cmd
		return writeTestReport(cmd)
	})
	require.NoError(t, err)
	close(calls)
	require.Len(t, calls, 2, "expected both modules and no E2E")
	for cmd := range calls {
		for _, arg := range []string{"-cover", "-args", "-run", "./e2e"} {
			require.NotContains(t, cmd.Args, arg, "unexpected argument in unit invocation")
		}
	}
}

func TestExecuteSkipsEmptyModules(t *testing.T) {
	selection := plan{E2ERunPattern: "^TestPG$"}
	settings := runSettings{logDir: t.TempDir()}
	calls := make(chan *exec.Cmd, 3)
	err := executePlan(selection, settings, func(cmd *exec.Cmd) error {
		calls <- cmd
		return writeTestReport(cmd)
	})
	require.NoError(t, err)
	require.Len(t, calls, 1, "empty package lists must not invoke go test in the current directory")
	require.Contains(t, (<-calls).Args, selection.E2ERunPattern, "expected owned E2E suites")
	data, err := os.ReadFile(filepath.Join(settings.logDir, "test-selection.json"))
	require.NoError(t, err)
	var recorded executionPlan
	require.NoError(t, json.Unmarshal(data, &recorded))
	require.Equal(t, []string{"e2e-test-results.xml"}, recorded.Reports)
}

func TestDecodeYAML(t *testing.T) {
	groups, err := decodeGroups([]byte(`# Ownership comments are supported.
postgres:
  packages: [connectors/postgres]
  e2e:
    - TestPG
`))
	require.NoError(t, err)
	require.Equal(t, []string{"TestPG"}, groups["postgres"].E2E)
	for _, input := range []string{
		"postgres:\n  pakcages: [connectors/postgres]\n",
		"postgres: {}\npostgres: {}\n",
		"postgres:\n  e2e: [TestPG]\n  e2e: [TestOther]\n",
		"postgres: {}\n---\nmysql: {}\n",
	} {
		_, err := decodeGroups([]byte(input))
		require.Error(t, err, "invalid configuration accepted: %s", input)
	}
}

func writeTestReport(cmd *exec.Cmd) error {
	report := cmd.Args[slices.Index(cmd.Args, "--junitfile")+1]
	return os.WriteFile(report, []byte("<testsuites/>"), 0o600)
}
