package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecutePlanContinuesAfterFailure(t *testing.T) {
	groups, packages, tests := fixtures()
	selection, err := selectTests(groups, packages, tests, "mysql")
	require.NoError(t, err)
	settings := runSettings{
		logDir:      filepath.Join(t.TempDir(), "logs with spaces"),
		coverageDir: filepath.Join(t.TempDir(), "coverage with spaces"),
	}
	failed := errors.New("test failed")
	var calls []*exec.Cmd
	err = executePlan(selection, settings, func(cmd *exec.Cmd) error {
		calls = append(calls, cmd)
		if len(calls) == 1 {
			data, err := os.ReadFile(filepath.Join(settings.logDir, "test-selection.json"))
			require.NoError(t, err)
			var recorded executionPlan
			require.NoError(t, json.Unmarshal(data, &recorded))
			require.Equal(t, selection, recorded.Selection)
			require.Equal(t, []string{"test-results.xml", "pkg-test-results.xml", "e2e-test-results.xml"}, recorded.Reports)
			return failed
		}
		return nil
	})
	require.ErrorIs(t, err, failed)
	require.ErrorContains(t, err, "test-results.xml")
	require.Len(t, calls, 3, "expected both modules and E2E to run despite failure")
	for i, cmd := range calls {
		if i < 2 {
			require.NotContains(t, cmd.Args, "-run", "unexpected test filter in call %d", i)
		}
		for _, arg := range []string{"-cover", "-test.gocoverdir=" + settings.coverageDir} {
			require.Contains(t, cmd.Args, arg, "call %d", i)
		}
	}
	require.Equal(t, "pkg", calls[1].Dir)
	require.Contains(t, calls[1].Args, "github.com/PeerDB-io/peerdb/flow/pkg/...")
	runIndex := slices.Index(calls[2].Args, "-run")
	require.GreaterOrEqual(t, runIndex, 0, "E2E ownership filter missing")
	require.Less(t, runIndex+1, len(calls[2].Args))
	require.Equal(t, selection.E2ERunPattern, calls[2].Args[runIndex+1])
	require.Contains(t, calls[2].Args, "./e2e")
	require.FileExists(t, filepath.Join(settings.logDir, "test-selection.json"))
}

func TestExecuteUnitWithoutCoverageOrE2E(t *testing.T) {
	groups, packages, tests := fixtures()
	selection, err := selectTests(groups, packages, tests, "unit")
	require.NoError(t, err)
	var calls []*exec.Cmd
	err = executePlan(selection, runSettings{logDir: t.TempDir()}, func(cmd *exec.Cmd) error {
		calls = append(calls, cmd)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, calls, 2, "expected both modules and no E2E")
	for _, cmd := range calls {
		for _, arg := range []string{"-cover", "-args", "-run", "./e2e"} {
			require.NotContains(t, cmd.Args, arg, "unexpected argument in unit invocation")
		}
	}
}

func TestExecuteSkipsEmptyModules(t *testing.T) {
	selection := plan{E2ERunPattern: "^TestPG$"}
	settings := runSettings{logDir: t.TempDir()}
	calls := 0
	err := executePlan(selection, settings, func(cmd *exec.Cmd) error {
		calls++
		require.Contains(t, cmd.Args, selection.E2ERunPattern, "expected owned E2E suites")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls, "empty package lists must not invoke go test in the current directory")
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
