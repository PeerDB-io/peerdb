package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeReports(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOWORK", "off")
	for name, value := range map[string]string{
		"WORKFLOW_RUN_ID": "42", "WORKFLOW_RUN_NUMBER": "2",
		"WORKFLOW_RUN_LINK": "https://example.invalid/42", "WORKFLOW_HEAD_BRANCH": "branch",
		"COMMIT_SHA": "abc", "COMBINATION_ID": "tilt-mysql",
	} {
		t.Setenv(name, value)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/reports\ngo 1.27.0\n"), 0o600))
	var reports []string
	for _, fixture := range []struct {
		name   string
		source string
		fails  bool
	}{
		{
			name: "flow",
			source: `
				package flow
				import "testing"
				func TestFailed(t *testing.T) { t.Error("a < b\nsecond line") }
				func TestPassed(t *testing.T) { t.Log("normal output") }
			`,
			fails: true,
		},
		{
			name: "pkg",
			source: `
				package pkg
				import "testing"
				func TestSkipped(t *testing.T) { t.Skip("unsupported & disabled") }
			`,
			fails: false,
		},
		{
			name: "e2e",
			source: `
				package e2e
				import "testing"
				func TestPassed(t *testing.T) {}
			`,
			fails: false,
		},
	} {
		pkgDir := filepath.Join(dir, fixture.name)
		require.NoError(t, os.Mkdir(pkgDir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(pkgDir, "fixture_test.go"), []byte(fixture.source), 0o600))
		report := fixture.name + ".xml"
		reports = append(reports, report)
		//nolint:gosec // Arguments are fixed fixture names and paths inside t.TempDir.
		cmd := exec.Command("gotestsum", "--junitfile", filepath.Join(dir, report), "--", "-count=1", "./"+fixture.name)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		if fixture.fails {
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "%s", output)
			require.Equal(t, 1, exitErr.ExitCode(), "%s", output)
			require.Contains(t, string(output), "a < b")
		} else {
			require.NoError(t, err, "%s", output)
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "unplanned.xml"), []byte("invalid"), 0o600))
	require.NoError(t, normalizeReports(dir, reports))
	data, err := os.ReadFile(filepath.Join(dir, "normalized-test-results.ndjson"))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	want := map[string]string{
		"example.com/reports/flow/TestFailed": "failure",
		"example.com/reports/flow/TestPassed": "success",
		"example.com/reports/pkg/TestSkipped": "skipped",
		"example.com/reports/e2e/TestPassed":  "success",
	}
	require.Len(t, lines, len(want))
	for _, line := range lines {
		var actual map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &actual))
		for key, value := range map[string]any{
			"workflow_run_id": float64(42), "workflow_retry_number": float64(2),
			"workflow_run_link": "https://example.invalid/42", "workflow_head_branch": "branch",
			"commit_sha": "abc", "compatibility_matrix_id": "tilt-mysql",
		} {
			require.Equal(t, value, actual[key], key)
		}
		require.IsType(t, "", actual["suite_name"])
		require.IsType(t, "", actual["test"])
		key := actual["suite_name"].(string) + "/" + actual["test"].(string)
		require.Contains(t, want, key)
		require.Equal(t, want[key], actual["result"], key)
		delete(want, key)
		require.IsType(t, "", actual["timestamp"])
		_, err := time.Parse(time.RFC3339, actual["timestamp"].(string))
		require.NoError(t, err)
		require.IsType(t, float64(0), actual["duration_seconds"])
		require.GreaterOrEqual(t, actual["duration_seconds"].(float64), float64(0))
		switch actual["result"] {
		case "failure":
			require.Contains(t, actual["reason"], "a < b")
			require.Contains(t, actual["reason"], "second line")
		case "skipped":
			require.NotEmpty(t, actual["reason"])
		default:
			require.Nil(t, actual["reason"])
		}
	}
	require.Empty(t, want)
}

func TestNormalizeEmptyReportsWithoutCIMetadata(t *testing.T) {
	for _, name := range []string{"WORKFLOW_RUN_ID", "WORKFLOW_RUN_NUMBER", "COMBINATION_ID"} {
		t.Setenv(name, "")
	}
	for _, reports := range [][]string{nil, {"empty.xml"}} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "empty.xml"), []byte("<testsuites/>"), 0o600))
		require.NoError(t, normalizeReports(dir, reports))
		data, err := os.ReadFile(filepath.Join(dir, "normalized-test-results.ndjson"))
		require.NoError(t, err)
		require.Empty(t, data)
	}
}

func TestNormalizeReportsRejectsMissingOrInvalidReports(t *testing.T) {
	for _, content := range []string{
		"missing", "", "<testsuites><testsuite>", "<wrong/>", "<testsuites/><testsuites/>", "<testsuites/>garbage",
	} {
		t.Run(content, func(t *testing.T) {
			dir := t.TempDir()
			if content != "missing" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "report.xml"), []byte(content), 0o600))
			}
			err := normalizeReports(dir, []string{"report.xml"})
			require.ErrorContains(t, err, "report.xml")
			require.NoFileExists(t, filepath.Join(dir, "normalized-test-results.ndjson"))
		})
	}
}

func TestExecutePlanRejectsStaleReports(t *testing.T) {
	dir := t.TempDir()
	for _, report := range []string{"test-results.xml", "normalized-test-results.ndjson"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, report), []byte("<testsuites/>"), 0o600))
	}
	err := executePlan(plan{FlowPackages: []string{"./example"}}, runSettings{logDir: dir}, func(*exec.Cmd) error {
		return nil // The command produced no report.
	})
	require.ErrorContains(t, err, "test-results.xml")
	require.NoFileExists(t, filepath.Join(dir, "normalized-test-results.ndjson"))
}
