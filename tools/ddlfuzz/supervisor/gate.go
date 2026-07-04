package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type GateStep struct {
	Name    string
	Dir     string
	Timeout time.Duration
	Args    []string
}

type GateError struct {
	Step   string
	Result Result
	Err    error
}

func (e *GateError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("gate step %q failed: %v\n%s", e.Step, e.Err, resultOutputTail(e.Result, 8000))
}

func gateSteps(cfg Config) []GateStep {
	flow := filepath.Join(cfg.Root, "flow")
	return []GateStep{
		{Name: "flow go build", Dir: flow, Timeout: 10 * time.Minute, Args: []string{"go", "build", "./..."}},
		{Name: "flow go vet mysql", Dir: flow, Timeout: 5 * time.Minute, Args: []string{"go", "vet", "./connectors/mysql/"}},
		{Name: "flow mysql ddl tests", Dir: flow, Timeout: 10 * time.Minute, Args: []string{"go", "test", "./connectors/mysql/", "-run", "TestDDL|TestProcessRenameTableQueryMetric|TestClassifyOnlineSchemaMigrationTool", "-count=1"}},
		{Name: "flow golangci-lint mysql", Dir: flow, Timeout: 10 * time.Minute, Args: []string{"golangci-lint", "run", "./connectors/mysql/..."}},
		{Name: "ddlfuzz go test", Dir: cfg.DDLDir, Timeout: 15 * time.Minute, Args: []string{"go", "test", "./..."}},
		{Name: "ddlfuzz go build", Dir: cfg.DDLDir, Timeout: 10 * time.Minute, Args: []string{"go", "build", "./..."}},
	}
}

func RunGate(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Minute)
	defer cancel()
	for _, step := range gateSteps(cfg) {
		args := append([]string{"-n", "10"}, step.Args...)
		res, err := RunTimeout(ctx, step.Dir, step.Timeout, nil, "nice", args...)
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return &GateError{Step: step.Name, Result: res, Err: err}
		}
	}
	return nil
}

func RunGolden(ctx context.Context, cfg Config) error {
	res, err := RunTimeout(ctx, cfg.DDLDir, 20*time.Minute, nil, cfg.DDLfuzzBin, "golden")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("ddlfuzz golden failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	return nil
}

func RebuildOracles(ctx context.Context, cfg Config, engines []string) error {
	seen := make(map[string]bool)
	for _, engine := range engines {
		engine = strings.ToLower(engine)
		if seen[engine] {
			continue
		}
		seen[engine] = true
		var script string
		switch engine {
		case "mysql":
			script = cfg.MySQLBuild
		case "mariadb":
			script = cfg.MariaBuild
		default:
			return fmt.Errorf("unknown oracle engine %q", engine)
		}
		res, err := RunTimeout(ctx, cfg.DDLDir, 30*time.Minute, nil, script)
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return fmt.Errorf("%s oracle build failed: %w\n%s", engine, err, resultOutputTail(res, 8000))
		}
	}
	return nil
}

func touchedOracleEngines(paths []string) []string {
	seen := make(map[string]bool)
	for _, p := range paths {
		switch {
		case strings.HasPrefix(p, "tools/ddlfuzz/oracle/mysql/"):
			seen["mysql"] = true
		case strings.HasPrefix(p, "tools/ddlfuzz/oracle/mariadb/"):
			seen["mariadb"] = true
		}
	}
	out := make([]string, 0, len(seen))
	for engine := range seen {
		out = append(out, engine)
	}
	return out
}

func touchedParserOrOracle(paths []string) bool {
	for _, p := range paths {
		switch {
		case strings.HasPrefix(p, "flow/connectors/mysql/"):
			return true
		case strings.HasPrefix(p, "tools/ddlfuzz/oracle/"):
			return true
		}
	}
	return false
}

func touchesE2EBinary(paths []string) bool {
	for _, p := range paths {
		switch {
		case p == "tools/ddlfuzz/go.mod", p == "tools/ddlfuzz/go.sum":
			return true
		case strings.HasPrefix(p, "flow/"):
			return true
		case strings.HasPrefix(p, "tools/ddlfuzz/e2e/"):
			return true
		case strings.HasPrefix(p, "tools/ddlfuzz/cmd/ddlfuzz-e2e/"):
			return true
		case strings.HasPrefix(p, "tools/ddlfuzz/internal/"):
			return true
		}
	}
	return false
}
