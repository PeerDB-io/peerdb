package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type SelfRestartMarker struct {
	ValidatedHead string    `json:"validated_head"`
	ID            string    `json:"id,omitempty"`
	TS            time.Time `json:"ts"`
}

func selfcheckCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper selfcheck", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.ensureStateDirs(); err != nil {
		return err
	}
	fmt.Printf("ddlsuper selfcheck ok build=%s state=%s\n", buildStamp(), cfg.StateDir)
	return nil
}

// execRestartPending tells runCommand's shutdown path to exec the rebuilt
// binary (after the children's graceful-stop handshakes) instead of writing a
// final report and composing down.
var execRestartPending atomic.Bool

func handleSelfRestart(ctx context.Context, cfg Config, req MergeRequest, validatedHead string, paths []string, shutdown func(), noExec bool) (string, string) {
	if !touchesSupervisor(paths) {
		return mergeSupervisorRestartNone, ""
	}
	newBin := filepath.Join(cfg.BuildDir, "ddlsuper.new")
	res, err := RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, "go", "build", "-o", newBin, "./supervisor")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		_ = writeRestartRequired(cfg, validatedHead)
		return mergeSupervisorRestartRequired, fmt.Sprintf("ddlsuper rebuild failed: %v\n%s", err, resultOutputTail(res, 8000))
	}
	res, err = RunTimeout(ctx, cfg.DDLDir, time.Minute, nil, newBin, "selfcheck")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		_ = writeRestartRequired(cfg, validatedHead)
		return mergeSupervisorRestartRequired, fmt.Sprintf("ddlsuper selfcheck failed: %v\n%s", err, resultOutputTail(res, 4000))
	}
	if err := os.Rename(newBin, filepath.Join(cfg.BuildDir, "ddlsuper")); err != nil {
		_ = writeRestartRequired(cfg, validatedHead)
		return mergeSupervisorRestartRequired, err.Error()
	}
	if noExec {
		if err := writeRestartRequired(cfg, validatedHead); err != nil {
			return mergeSupervisorRestartRequired, err.Error()
		}
		return mergeSupervisorRestartRequired, "ddlsuper rebuilt; no supervisor running, next run fast-resumes"
	}
	if !req.Restart {
		if err := writeRestartRequired(cfg, validatedHead); err != nil {
			return mergeSupervisorRestartRequired, err.Error()
		}
		return mergeSupervisorRestartRequired, "supervisor changed; restart required (--no-restart)"
	}
	if err := atomicWriteJSON(filepath.Join(cfg.StateDir, "selfrestart.json"), SelfRestartMarker{ValidatedHead: validatedHead, ID: req.ID, TS: time.Now().UTC()}, 0o644); err != nil {
		_ = writeRestartRequired(cfg, validatedHead)
		return mergeSupervisorRestartRequired, err.Error()
	}
	execRestartPending.Store(true)
	if shutdown != nil {
		shutdown()
	}
	return mergeSupervisorRestartRestarting, "supervisor exec after graceful shutdown"
}

func execSelfRestart(cfg Config) error {
	bin, err := filepath.Abs(filepath.Join(cfg.BuildDir, "ddlsuper"))
	if err != nil {
		return err
	}
	env := append(os.Environ(), "DDLFUZZ_RESUME=1")
	return syscall.Exec(bin, os.Args, env)
}

func writeRestartRequired(cfg Config, head string) error {
	return atomicWriteJSON(filepath.Join(cfg.StateDir, "RESTART_REQUIRED"), SelfRestartMarker{ValidatedHead: head, TS: time.Now().UTC()}, 0o644)
}

func touchesSupervisor(paths []string) bool {
	for _, p := range paths {
		if strings.HasPrefix(filepath.ToSlash(p), "tools/ddlfuzz/supervisor/") {
			return true
		}
	}
	return false
}

// initRunStart decides which clock this run lives on. A resume (self-restart
// or RESTART_REQUIRED fast path) continues the persisted campaign start so the
// deadline survives the handover; any other start is a fresh campaign and
// overwrites run-start — a stale file from a finished campaign must never
// shorten a new one.
func initRunStart(cfg *Config) error {
	if _, _, ok := resumeMarker(*cfg); ok {
		applyPersistedRunStart(cfg)
		return nil
	}
	return atomicWriteFile(filepath.Join(cfg.StateDir, "run-start"), []byte(cfg.StartedAt.UTC().Format(time.RFC3339)+"\n"), 0o644)
}

func loadRunStart(cfg Config) (time.Time, error) {
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "run-start"))
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, strings.TrimSpace(string(data)))
}

func applyPersistedRunStart(cfg *Config) {
	start, err := loadRunStart(*cfg)
	if err != nil {
		return
	}
	cfg.StartedAt = start
	cfg.Deadline = computeDeadline(start, cfg.RunHours, os.Getenv("DDLFUZZ_HOURS") != "")
}

func resumeMarker(cfg Config) (SelfRestartMarker, string, bool) {
	for _, name := range []string{"selfrestart.json", "RESTART_REQUIRED"} {
		var marker SelfRestartMarker
		path := filepath.Join(cfg.StateDir, name)
		if err := readJSONFile(path, &marker); err == nil {
			return marker, path, true
		}
	}
	return SelfRestartMarker{}, "", false
}

func resumePreflight(ctx context.Context, cfg *Config, logf func(string, ...any)) (bool, error) {
	if os.Getenv("DDLFUZZ_RESUME") != "1" {
		if _, _, ok := resumeMarker(*cfg); !ok {
			return false, nil
		}
	}
	marker, markerPath, ok := resumeMarker(*cfg)
	if !ok {
		return false, nil
	}
	applyPersistedRunStart(cfg)
	if err := PopulateGoEnv(ctx, cfg); err != nil {
		return true, err
	}
	if err := HelloSmoke(ctx, cfg.MySQLOracle, "mysql"); err != nil {
		return true, fmt.Errorf("mysql hello: %w", err)
	}
	if err := HelloSmoke(ctx, cfg.MariaOracle, "mariadb"); err != nil {
		return true, fmt.Errorf("mariadb hello: %w", err)
	}
	head, err := GitHead(ctx, *cfg)
	if err != nil {
		return true, err
	}
	if head != marker.ValidatedHead {
		if logf != nil {
			logf("resume marker head mismatch: HEAD=%s validated=%s; full preflight required", head, marker.ValidatedHead)
		}
		_ = os.Remove(markerPath)
		return false, nil
	}
	if err := ensureComposeHealthy(ctx, *cfg); err != nil {
		return true, err
	}
	_ = os.Remove(markerPath)
	if logf != nil {
		logf("resume preflight accepted validated head %s", marker.ValidatedHead)
	}
	return true, nil
}

func ensureComposeHealthy(ctx context.Context, cfg Config) error {
	res, err := RunTimeout(ctx, cfg.Root, 2*time.Minute, nil, "docker", "compose", "-f", cfg.ComposeFile, "ps", "--status", "running")
	if err == nil && res.ExitCode == 0 && strings.TrimSpace(res.Stdout) != "" {
		return nil
	}
	res, err = RunTimeout(ctx, cfg.Root, 5*time.Minute, nil, "docker", "compose", "-f", cfg.ComposeFile, "up", "-d", "--wait")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("docker compose up failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	return nil
}
