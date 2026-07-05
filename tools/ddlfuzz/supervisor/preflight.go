package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type SupervisorLock struct {
	file *os.File
	path string
}

func AcquireSupervisorLock(cfg Config) (*SupervisorLock, error) {
	path := filepath.Join(cfg.StateDir, "supervisor.pid")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another ddlsuper appears to be running: %w", err)
	}
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &SupervisorLock{file: f, path: path}, nil
}

func (l *SupervisorLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	err := l.file.Close()
	_ = os.Remove(l.path)
	return err
}

func RunPreflight(ctx context.Context, cfg *Config, logf func(string, ...any)) error {
	start := time.Now()
	step := func(name string, fn func() error) error {
		t0 := time.Now()
		if logf != nil {
			logf("preflight %s: start", name)
		}
		if err := fn(); err != nil {
			if logf != nil {
				logf("preflight %s: failed after %s: %v", name, time.Since(t0).Round(time.Second), err)
			}
			return fmt.Errorf("%s: %w", name, err)
		}
		if logf != nil {
			logf("preflight %s: ok in %s", name, time.Since(t0).Round(time.Second))
		}
		return nil
	}

	_ = os.Remove(filepath.Join(cfg.StateDir, "BLOCKED"))
	if _, err := RecoverMergeInflight(ctx, *cfg); err != nil {
		return fmt.Errorf("merge crash recovery: %w", err)
	}
	if resumed, err := resumePreflight(ctx, cfg, logf); resumed || err != nil {
		return err
	}

	if err := step("repo state", func() error {
		branch, err := GitBranch(ctx, *cfg)
		if err != nil {
			return err
		}
		if branch != "parser-wip" {
			return fmt.Errorf("branch=%q, want parser-wip", branch)
		}
		clean, dirty, err := GitTrackedClean(ctx, *cfg)
		if err != nil {
			return err
		}
		if !clean {
			return fmt.Errorf("tracked tree dirty:\n%s", dirty)
		}
		untracked, err := GitUntrackedSet(ctx, *cfg)
		if err != nil {
			return err
		}
		return writeUntrackedBaseline(*cfg, untracked)
	}); err != nil {
		return err
	}

	if err := step("attempt drift", func() error {
		return handlePreflightDrift(ctx, *cfg, logf)
	}); err != nil {
		return err
	}

	if err := step("tools", func() error {
		for _, tool := range []string{"codex", "docker", "jq", "golangci-lint", "go"} {
			if _, err := exec.LookPath(tool); err != nil {
				return fmt.Errorf("%s not on PATH: %w", tool, err)
			}
		}
		res, err := RunTimeout(ctx, cfg.Root, time.Minute, nil, "docker", "info")
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return fmt.Errorf("docker info failed: %w\n%s", err, resultOutputTail(res, 4000))
		}
		return PopulateGoEnv(ctx, cfg)
	}); err != nil {
		return err
	}

	if err := step("disk", func() error {
		free, err := FreeBytes(cfg.StateDir)
		if err != nil {
			return err
		}
		if free < 40*GiB {
			return fmt.Errorf("free space %s < 40GiB", formatGiB(free))
		}
		return nil
	}); err != nil {
		return err
	}

	if err := step("builds", func() error {
		if err := os.MkdirAll(cfg.BuildDir, 0o755); err != nil {
			return err
		}
		res, err := RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, "go", "build", "-o", cfg.DDLfuzzBin, "./cmd/ddlfuzz")
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return fmt.Errorf("go build ddlfuzz failed: %w\n%s", err, resultOutputTail(res, 8000))
		}
		res, err = RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, "go", "build", "-o", cfg.E2EBin, "./cmd/ddlfuzz-e2e")
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return fmt.Errorf("go build ddlfuzz-e2e failed: %w\n%s", err, resultOutputTail(res, 8000))
		}
		if _, err := os.Stat(cfg.MySQLOracle); os.IsNotExist(err) {
			if err := RebuildOracles(ctx, *cfg, []string{"mysql"}); err != nil {
				return err
			}
		}
		if _, err := os.Stat(cfg.MariaOracle); os.IsNotExist(err) {
			if err := RebuildOracles(ctx, *cfg, []string{"mariadb"}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := step("oracle hello", func() error {
		if err := HelloSmoke(ctx, cfg.MySQLOracle, "mysql"); err != nil {
			return fmt.Errorf("mysql hello: %w", err)
		}
		if err := HelloSmoke(ctx, cfg.MariaOracle, "mariadb"); err != nil {
			return fmt.Errorf("mariadb hello: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := step("codex auth", func() error {
		return CodexAuthSmoke(ctx, *cfg)
	}); err != nil {
		return err
	}

	if err := step("golden", func() error {
		return RunGolden(ctx, *cfg)
	}); err != nil {
		return err
	}

	if err := step("gate", func() error {
		return RunGate(ctx, *cfg)
	}); err != nil {
		return err
	}

	if err := step("record last_good_commit", func() error {
		head, err := GitHead(ctx, *cfg)
		if err != nil {
			return err
		}
		return atomicWriteFile(filepath.Join(cfg.StateDir, "last_good_commit"), []byte(head+"\n"), 0o644)
	}); err != nil {
		return err
	}

	if err := step("e2e compose up", func() error {
		res, err := RunTimeout(ctx, cfg.Root, 5*time.Minute, nil, "docker", "compose", "-f", cfg.ComposeFile, "up", "-d", "--wait")
		if err != nil || res.ExitCode != 0 {
			if err == nil {
				err = fmt.Errorf("exit code %d", res.ExitCode)
			}
			return fmt.Errorf("docker compose up failed: %w\n%s", err, resultOutputTail(res, 8000))
		}
		return nil
	}); err != nil {
		return err
	}

	if logf != nil {
		logf("preflight complete in %s", time.Since(start).Round(time.Second))
	}
	return nil
}

type driftClass string

const (
	driftNone    driftClass = "none"
	driftResidue driftClass = "residue"
	driftHuman   driftClass = "human"
	driftBlocked driftClass = "blocked"
)

func handlePreflightDrift(ctx context.Context, cfg Config, logf func(string, ...any)) error {
	lastGood, err := loadLastGoodCommit(cfg)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	head, err := GitHead(ctx, cfg)
	if err != nil {
		return err
	}
	if head == lastGood {
		return nil
	}
	commits, err := GitCommitsSince(ctx, cfg, lastGood)
	if err != nil {
		return err
	}
	class, detail, err := classifyPreflightDrift(cfg, commits)
	if err != nil {
		return err
	}
	switch class {
	case driftNone:
		return nil
	case driftResidue:
		if logf != nil {
			logf("preflight drift: resetting %d failed-attempt residue commit(s) to %s", len(commits), lastGood)
		}
		return GitResetHard(ctx, cfg, lastGood)
	case driftHuman:
		if logf != nil {
			logf("preflight drift: keeping human commit(s): %s", detail)
		}
		return nil
	default:
		return fmt.Errorf("HEAD drift from last_good_commit is ambiguous; resolve manually: %s", detail)
	}
}

func classifyPreflightDrift(cfg Config, commits []Commit) (driftClass, string, error) {
	if len(commits) == 0 {
		return driftNone, "", nil
	}
	attributed, unreadable, err := residueCommitSet(cfg)
	if err != nil {
		return driftBlocked, err.Error(), nil
	}
	if len(unreadable) > 0 {
		return driftBlocked, "unreadable attempt records: " + strings.Join(unreadable, ", "), nil
	}
	var residue, unknown []string
	for _, c := range commits {
		label := c.SHA
		if c.Subject != "" {
			label += " " + c.Subject
		}
		if attributed[c.SHA] {
			residue = append(residue, label)
		} else {
			unknown = append(unknown, label)
		}
	}
	switch {
	case len(residue) == len(commits):
		return driftResidue, strings.Join(residue, ", "), nil
	case len(unknown) == len(commits):
		if currentAttemptExists(cfg) {
			return driftBlocked, "current-attempt.json exists with unknown commit(s): " + strings.Join(unknown, ", "), nil
		}
		return driftHuman, strings.Join(unknown, ", "), nil
	default:
		return driftBlocked, "attempt residue: " + strings.Join(residue, ", ") + "; unknown: " + strings.Join(unknown, ", "), nil
	}
}

func residueCommitSet(cfg Config) (map[string]bool, []string, error) {
	out := make(map[string]bool)
	attemptsDir := filepath.Join(cfg.StateDir, "attempts")
	entries, err := os.ReadDir(attemptsDir)
	if os.IsNotExist(err) {
		return out, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var unreadable []string
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(attemptsDir, ent.Name())
		records, err := LoadAttemptRecords(path)
		if err != nil {
			unreadable = append(unreadable, ent.Name())
			continue
		}
		for _, rec := range records {
			if rec.Outcome == "fixed" || rec.Outcome == "ledgered" {
				continue
			}
			for _, sha := range rec.Commits {
				if strings.TrimSpace(sha) != "" {
					out[strings.TrimSpace(sha)] = true
				}
			}
		}
	}
	sort.Strings(unreadable)
	return out, unreadable, nil
}

func currentAttemptExists(cfg Config) bool {
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "current-attempt.json"))
	if err != nil {
		return false
	}
	var cur CurrentAttempt
	return json.Unmarshal(data, &cur) == nil && cur.Sig != ""
}

func PopulateGoEnv(ctx context.Context, cfg *Config) error {
	res, err := RunTimeout(ctx, cfg.Root, 30*time.Second, nil, "go", "env", "GOCACHE", "GOMODCACHE")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("go env GOCACHE GOMODCACHE failed: %w\n%s", err, resultOutputTail(res, 4000))
	}
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	if len(lines) < 2 {
		return fmt.Errorf("go env returned %d lines, want 2", len(lines))
	}
	cfg.GoCache = strings.TrimSpace(lines[0])
	cfg.GoModCache = strings.TrimSpace(lines[1])
	return nil
}

func CodexAuthSmoke(ctx context.Context, cfg Config) error {
	path := filepath.Join(cfg.StateDir, "attempts", "codex-smoke.last.txt")
	_ = os.Remove(path)
	args := codexArgs(cfg, "read-only", path)
	res, err := RunTimeout(ctx, cfg.Root, 120*time.Second, strings.NewReader("Reply with the single word ok"), "codex", args...)
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("codex auth smoke failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !strings.Contains(strings.ToLower(string(data)), "ok") {
		return fmt.Errorf("codex auth smoke last message did not contain ok: %q", tailString(string(data), 1000))
	}
	return nil
}

func WriteBlocked(cfg Config, err error) {
	msg := fmt.Sprintf("blocked_at: %s\nreason:\n%s\n", time.Now().UTC().Format(time.RFC3339), err.Error())
	_ = atomicWriteFile(filepath.Join(cfg.StateDir, "BLOCKED"), []byte(msg), 0o644)
}
