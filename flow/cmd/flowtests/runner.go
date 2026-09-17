package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type runSettings struct {
	logDir      string
	coverageDir string
}

type executionPlan struct {
	Selection plan     `json:"selection"`
	Reports   []string `json:"reports"`
}

func executePlan(selection plan, settings runSettings, execute func(*exec.Cmd) error) error {
	if settings.logDir == "" {
		settings.logDir = "../logs"
	}
	logDir, err := prepareDirectory(settings.logDir)
	if err != nil {
		return err
	}
	// CI supplies a directory outside the source tree to avoid triggering Tilt
	// rebuilds. Local runs need no coverage setup.
	if settings.coverageDir != "" {
		settings.coverageDir, err = prepareDirectory(settings.coverageDir)
		if err != nil {
			return err
		}
	}
	var commands []*exec.Cmd
	expected := executionPlan{Selection: selection, Reports: []string{}}
	addPackages := func(module, report, timeout string, packages, filters []string) {
		if len(packages) == 0 {
			return
		}
		args := []string{"--format", "standard-quiet", "--no-color", "--junitfile", filepath.Join(logDir, report), "--"}
		if settings.coverageDir != "" {
			coverpkg := "github.com/PeerDB-io/peerdb/flow/"
			if module == "pkg" {
				coverpkg += "pkg/"
			}
			args = append(args, "-cover", "-coverpkg", coverpkg+"...")
		}
		args = append(args, "-p", "32")
		args = append(args, packages...)
		args = append(args, filters...)
		args = append(args, "-timeout", timeout)
		if settings.coverageDir != "" {
			args = append(args, "-args", "-test.gocoverdir="+settings.coverageDir)
		}
		cmd := exec.Command("gotestsum", args...)
		cmd.Dir = module
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		commands = append(commands, cmd)
		expected.Reports = append(expected.Reports, report)
	}
	addPackages(".", "test-results.xml", "1200s", selection.FlowPackages, nil)
	addPackages("pkg", "pkg-test-results.xml", "300s", selection.PkgPackages, nil)
	if selection.E2ERunPattern != "" {
		addPackages(".", "e2e-test-results.xml", "1200s", []string{"./e2e"},
			[]string{"-run", selection.E2ERunPattern})
	}
	// Persist every expected report before starting any command, so ingestion
	// can distinguish unplanned reports from missing results after a failure.
	data, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(logDir, "test-selection.json"), append(data, '\n'), 0o600); err != nil {
		return err
	}
	// Continue after failures so both modules and E2E produce their reports.
	var failures []error
	for i, cmd := range commands {
		if err := execute(cmd); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", expected.Reports[i], err))
		}
	}
	return errors.Join(failures...)
}

func prepareDirectory(name string) (string, error) {
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", err
	}
	return abs, nil
}
