package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	go http.ListenAndServe("localhost:6062", nil)
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	cfg, err := LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		if err := runCommand(cfg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	case "hello":
		if err := helloCommand(cfg, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "gate":
		if err := RunGate(context.Background(), cfg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "fix-once":
		if err := fixOnceCommand(cfg, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "status":
		if err := statusCommand(cfg, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "watch":
		if err := watchCommand(cfg, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "merge-staged":
		if err := mergeStagedCommand(cfg, os.Args[2:]); err != nil {
			if err.Error() != "" {
				fmt.Fprintln(os.Stderr, err)
			}
			os.Exit(exitCodeForError(err, 1))
		}
	case "merge-cancel":
		if err := mergeCancelCommand(cfg, os.Args[2:]); err != nil {
			if err.Error() != "" {
				fmt.Fprintln(os.Stderr, err)
			}
			os.Exit(exitCodeForError(err, 1))
		}
	case "oracle-manifest":
		if err := oracleManifestCommand(cfg, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "selfcheck":
		if err := selfcheckCommand(cfg, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
}

func usage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage: ddlsuper run|hello|gate|fix-once|status|watch|merge-staged|merge-cancel|oracle-manifest|selfcheck")
}

func helloCommand(_ Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper hello", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var oracle, engine string
	fs.StringVar(&oracle, "oracle", "", "oracle binary path")
	fs.StringVar(&engine, "engine", "", "mysql or mariadb")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if oracle == "" || engine == "" {
		return fmt.Errorf("hello requires --oracle and --engine")
	}
	return HelloSmoke(context.Background(), oracle, engine)
}

func fixOnceCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper fix-once", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var sig string
	var skipFuzzer bool
	fs.StringVar(&sig, "sig", "", "finding signature")
	fs.BoolVar(&skipFuzzer, "skip-fuzzer", false, "skip hot-restart coordination")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if sig == "" {
		return fmt.Errorf("fix-once requires --sig")
	}
	if err := cfg.ensureStateDirs(); err != nil {
		return err
	}
	if err := PopulateGoEnv(context.Background(), &cfg); err != nil {
		return err
	}
	logger, closeLog, err := newSupervisorLogger(cfg)
	if err != nil {
		return err
	}
	defer closeLog()
	return FixOnce(context.Background(), cfg, sig, skipFuzzer, nil, nil, nil, logger)
}

func runCommand(cfg Config) error {
	if err := cfg.ensureStateDirs(); err != nil {
		return err
	}
	logger, closeLog, err := newSupervisorLogger(cfg)
	if err != nil {
		return err
	}
	defer closeLog()
	lock, err := acquireSupervisorLockRetry(cfg, 15*time.Second)
	if err != nil {
		WriteBlocked(cfg, err)
		return err
	}
	defer func() { _ = lock.Close() }()
	if err := killOrphanedChildren(cfg, logger); err != nil {
		WriteBlocked(cfg, err)
		return err
	}
	initChildTracking(cfg)
	if err := WriteRunState(cfg); err != nil {
		return err
	}
	if err := initRunStart(&cfg); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, cfg.Deadline)
	defer deadlineCancel()

	if err := RunPreflight(deadlineCtx, &cfg, logger); err != nil {
		WriteBlocked(cfg, err)
		return err
	}
	if err := WriteRunState(cfg); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(cfg.StateDir, "current-attempt.json"))
	fmt.Printf("preflight OK - supervising until deadline (%.2fh); Ctrl-C = graceful shutdown; status: build/ddlsuper status. Tip: run inside tmux.\n", cfg.RunHours)
	logger("preflight OK - supervising until %s", cfg.Deadline.Format(time.RFC3339))

	fuzzer := NewFuzzerManager(cfg, logger)
	e2e := NewE2EManager(cfg, logger)
	resumeMergeSvc := NewMergeService(cfg, fuzzer, e2e, logger)
	resumeMergeSvc.shutdown = cancel
	_ = serviceMergeSlot(deadlineCtx, cfg, resumeMergeSvc, logger)
	var wg sync.WaitGroup
	wg.Add(6)
	go func() { defer wg.Done(); fuzzer.Run(deadlineCtx) }()
	go func() { defer wg.Done(); e2e.Run(deadlineCtx) }()
	go func() { defer wg.Done(); RunFixLoop(deadlineCtx, cfg, fuzzer, e2e, logger, cancel) }()
	go func() { defer wg.Done(); runReportTickers(deadlineCtx, cfg, fuzzer, e2e, logger) }()
	go func() { defer wg.Done(); runDiskWatchdog(deadlineCtx, cfg, e2e, logger) }()
	go func() { defer wg.Done(); runOracleCrossCheck(deadlineCtx, cfg, logger) }()
	<-deadlineCtx.Done()
	logger("shutdown requested: %v", deadlineCtx.Err())
	fuzzer.Stop()
	e2e.Stop()
	if execRestartPending.Load() {
		wg.Wait()
		if children, err := loadChildRegistry(childRegistryPath(cfg)); err == nil && len(children) > 0 {
			logger("warning: children registry non-empty at exec handoff: %d entries", len(children))
		}
		logger("exec handoff to rebuilt ddlsuper")
		closeLog()
		_ = lock.Close()
		err := execSelfRestart(cfg)
		// exec only returns on failure; the merge stands and the marker
		// survives, so the next manual run fast-resumes.
		fmt.Fprintf(os.Stderr, "self-restart exec failed: %v; restart manually (state/selfrestart.json preserved)\n", err)
		return err
	}
	_ = composeDown(context.Background(), cfg)
	_ = WriteReport(cfg, "final", mergeSnapshots(fuzzer.Snapshot(), e2e.Snapshot()))
	wg.Wait()
	return nil
}

// acquireSupervisorLockRetry tolerates the momentary lock windows around a
// self-restart: the exec'd process reacquiring can collide with a merge-staged
// CLI's liveness probe (which acquires and releases the flock to test it).
func acquireSupervisorLockRetry(cfg Config, patience time.Duration) (*SupervisorLock, error) {
	deadline := time.Now().Add(patience)
	for {
		lock, err := AcquireSupervisorLock(cfg)
		if err == nil {
			return lock, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func newSupervisorLogger(cfg Config) (func(string, ...any), func(), error) {
	path := filepath.Join(cfg.StateDir, "supervisor.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	var mu sync.Mutex
	logf := func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		mu.Lock()
		defer mu.Unlock()
		_, _ = fmt.Fprintf(f, "%s %s\n", time.Now().UTC().Format(time.RFC3339), line)
	}
	return logf, func() { _ = f.Close() }, nil
}

type FuzzerManager struct {
	cfg      Config
	logf     func(string, ...any)
	mu       sync.Mutex
	proc     *Proc
	up       bool
	restarts int
	degraded bool
}

func NewFuzzerManager(cfg Config, logf func(string, ...any)) *FuzzerManager {
	return &FuzzerManager{cfg: cfg, logf: logf}
}

func (m *FuzzerManager) Run(ctx context.Context) {
	backoffs := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 135 * time.Second, 300 * time.Second}
	var recent []time.Time
	backoffIdx := 0
	for ctx.Err() == nil {
		p, err := Start(m.cfg.DDLDir, logWriter{m.logf, "fuzzer"}, logWriter{m.logf, "fuzzer"}, m.cfg.DDLfuzzBin, "fuzz", "--state", m.cfg.StateDir)
		if err != nil {
			m.logf("fuzzer start failed: %v", err)
			sleepContext(ctx, nextBackoff(backoffs, &backoffIdx))
			continue
		}
		m.setProc(p, true)
		wedge := time.NewTicker(time.Minute)
	waitLoop:
		for {
			select {
			case <-ctx.Done():
				wedge.Stop()
				p.StopGracefully(60 * time.Second)
				m.setProc(nil, false)
				return
			case <-p.Done():
				wedge.Stop()
				m.setProc(nil, false)
				m.logf("fuzzer exited: %v", p.Err())
				break waitLoop
			case <-wedge.C:
				if statsStale(filepath.Join(m.cfg.StateDir, "stats.json"), "ts", 5*time.Minute) {
					m.logf("fuzzer stats stale; restarting")
					p.StopGracefully(60 * time.Second)
				}
			}
		}
		m.mu.Lock()
		m.restarts++
		m.mu.Unlock()
		now := time.Now()
		recent = append(recent, now)
		recent = pruneTimes(recent, now.Add(-30*time.Minute))
		if len(recent) >= 6 {
			m.mu.Lock()
			m.degraded = true
			m.mu.Unlock()
			_ = writeRunEscalation(m.cfg, "run-fuzzer-crashloop.md", "fuzzer exited at least 6 times within 30 minutes; retrying every 30 minutes")
			sleepContext(ctx, 30*time.Minute)
			recent = nil
			continue
		}
		sleepContext(ctx, nextBackoff(backoffs, &backoffIdx))
	}
}

func (m *FuzzerManager) HotRestart(ctx context.Context, newBin string) error {
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if p != nil {
		p.StopGracefully(60 * time.Second)
	}
	if err := os.Rename(newBin, m.cfg.DDLfuzzBin); err != nil {
		return err
	}
	return nil
}

func (m *FuzzerManager) Stop() {
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if p != nil {
		p.StopGracefully(60 * time.Second)
	}
}

func (m *FuzzerManager) setProc(p *Proc, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.proc = p
	m.up = up
}

func (m *FuzzerManager) Snapshot() ComponentSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	free, _ := FreeBytes(m.cfg.StateDir)
	return ComponentSnapshot{FuzzerUp: m.up, FuzzerRestarts: m.restarts, DiskFreeBytes: free, Degraded: m.degraded}
}

type E2EManager struct {
	cfg      Config
	logf     func(string, ...any)
	mu       sync.Mutex
	proc     *Proc
	up       bool
	restarts int
	degraded bool
	disabled bool
}

func NewE2EManager(cfg Config, logf func(string, ...any)) *E2EManager {
	return &E2EManager{cfg: cfg, logf: logf}
}

func (m *E2EManager) Run(ctx context.Context) {
	backoffs := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 135 * time.Second, 300 * time.Second}
	var recent []time.Time
	backoffIdx := 0
	for ctx.Err() == nil {
		if m.isDisabled() {
			sleepContext(ctx, time.Minute)
			continue
		}
		if err := composeUp(ctx, m.cfg); err != nil {
			m.logf("e2e compose up failed: %v", err)
			sleepContext(ctx, nextBackoff(backoffs, &backoffIdx))
			continue
		}
		p, err := Start(m.cfg.DDLDir, logWriter{m.logf, "e2e"}, logWriter{m.logf, "e2e"}, m.cfg.E2EBin, "--state", m.cfg.StateDir)
		if err != nil {
			m.logf("e2e lane start failed: %v", err)
			sleepContext(ctx, nextBackoff(backoffs, &backoffIdx))
			continue
		}
		m.setProc(p, true)
		wedgeCheck := newE2EWedgeCheck(time.Now())
		wedge := time.NewTicker(time.Minute)
	waitLoop:
		for {
			select {
			case <-ctx.Done():
				wedge.Stop()
				p.StopGracefully(60 * time.Second)
				m.setProc(nil, false)
				return
			case <-p.Done():
				wedge.Stop()
				m.setProc(nil, false)
				m.logf("e2e lane exited: %v", p.Err())
				break waitLoop
			case <-wedge.C:
				if reason, wedged := wedgeCheck.check(filepath.Join(m.cfg.StateDir, "e2e-stats.json"), time.Now()); wedged {
					m.logf("e2e lane wedged (%s); restarting lane", reason)
					p.StopGracefully(60 * time.Second)
				}
			}
		}
		m.mu.Lock()
		m.restarts++
		m.mu.Unlock()
		now := time.Now()
		recent = append(recent, now)
		recent = pruneTimes(recent, now.Add(-30*time.Minute))
		if len(recent) >= 6 {
			m.mu.Lock()
			m.degraded = true
			m.mu.Unlock()
			_ = writeRunEscalation(m.cfg, "run-e2e-crashloop.md", "e2e lane exited at least 6 times within 30 minutes; retrying every 30 minutes")
			_ = composeDown(context.Background(), m.cfg)
			sleepContext(ctx, 30*time.Minute)
			recent = nil
			continue
		}
		sleepContext(ctx, nextBackoff(backoffs, &backoffIdx))
	}
}

func (m *E2EManager) Stop() {
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if p != nil {
		p.StopGracefully(60 * time.Second)
	}
}

func (m *E2EManager) HotRestart(ctx context.Context, newBin string) error {
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if p != nil {
		p.StopGracefully(60 * time.Second)
	}
	return os.Rename(newBin, m.cfg.E2EBin)
}

func (m *E2EManager) SetDisabled(disabled bool) {
	m.mu.Lock()
	changed := m.disabled != disabled
	m.disabled = disabled
	p := m.proc
	m.mu.Unlock()
	if changed && disabled {
		if p != nil {
			p.StopGracefully(60 * time.Second)
		}
		_ = composeDown(context.Background(), m.cfg)
	}
}

func (m *E2EManager) isDisabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.disabled
}

func (m *E2EManager) setProc(p *Proc, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.proc = p
	m.up = up
}

func (m *E2EManager) Snapshot() ComponentSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ComponentSnapshot{E2EUp: m.up, E2ERestarts: m.restarts, Degraded: m.degraded}
}

type logWriter struct {
	logf   func(string, ...any)
	prefix string
}

func (w logWriter) Write(p []byte) (int, error) {
	text := strings.TrimRight(string(p), "\n")
	if text != "" && w.logf != nil {
		for _, line := range strings.Split(text, "\n") {
			w.logf("%s: %s", w.prefix, line)
		}
	}
	return len(p), nil
}

func runReportTickers(ctx context.Context, cfg Config, f *FuzzerManager, e *E2EManager, logf func(string, ...any)) {
	report := time.NewTicker(cfg.ReportEvery)
	sample := time.NewTicker(cfg.SampleEvery)
	coverage := time.NewTicker(cfg.CoverageEvery)
	defer report.Stop()
	defer sample.Stop()
	defer coverage.Stop()
	_ = WriteReport(cfg, "running", mergeSnapshots(f.Snapshot(), e.Snapshot()))
	_ = AppendSample(cfg, CollectSample(cfg))
	for {
		select {
		case <-ctx.Done():
			return
		case <-report.C:
			if err := WriteReport(cfg, "running", mergeSnapshots(f.Snapshot(), e.Snapshot())); err != nil {
				logf("report write failed: %v", err)
			}
		case <-sample.C:
			if err := AppendSample(cfg, CollectSample(cfg)); err != nil {
				logf("sample append failed: %v", err)
			}
		case <-coverage.C:
			if err := AppendCoverageHistory(cfg); err != nil {
				logf("coverage history append failed: %v", err)
			}
		}
	}
}

func runDiskWatchdog(ctx context.Context, cfg Config, e *E2EManager, logf func(string, ...any)) {
	ticker := time.NewTicker(cfg.DiskEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			free, err := FreeBytes(cfg.StateDir)
			if err != nil {
				logf("disk watchdog failed: %v", err)
				continue
			}
			if free < 10*GiB {
				e.SetDisabled(true)
				_ = writeRunEscalation(cfg, "run-disk-low.md", fmt.Sprintf("free space %s < 10GiB; e2e disabled\n\n%s", formatGiB(free), DockerSystemDF(ctx, cfg)))
			} else if free > 20*GiB {
				e.SetDisabled(false)
			}
		}
	}
}

// runOracleCrossCheck implements reconciliation decision D7: periodically
// rotate the e2e lane's live-accepted sample and replay it through the parse
// oracles; an oracle reject of a live-accepted statement is an oracle-harness
// finding (class oracle-reject-live-accept, filed by the ddlfuzz binary).
// The rotated file is deleted only after a clean run.
func runOracleCrossCheck(ctx context.Context, cfg Config, logf func(string, ...any)) {
	ticker := time.NewTicker(cfg.CrossCheckEvery)
	defer ticker.Stop()
	src := filepath.Join(cfg.StateDir, "e2e-live-accepted.jsonl")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(src)
			if err != nil || info.Size() == 0 {
				continue
			}
			rotated := src + "." + time.Now().UTC().Format("20060102T150405") + ".rotated"
			if err := os.Rename(src, rotated); err != nil {
				logf("oracle cross-check rotate failed: %v", err)
				continue
			}
			res, err := RunTimeout(ctx, cfg.DDLDir, 30*time.Minute, nil,
				cfg.DDLfuzzBin, "replay", "--from", rotated, "--expect-accept")
			switch {
			case err == nil && res.ExitCode == 0:
				_ = os.Remove(rotated)
				logf("oracle cross-check clean: %s", strings.TrimSpace(res.Stdout))
			case err == nil && res.ExitCode == 10:
				logf("oracle cross-check filed findings: %s (kept %s)", strings.TrimSpace(res.Stdout), rotated)
			default:
				if err == nil {
					err = fmt.Errorf("exit code %d", res.ExitCode)
				}
				logf("oracle cross-check failed: %v (kept %s)\n%s", err, rotated, resultOutputTail(res, 4000))
			}
		}
	}
}

func composeUp(ctx context.Context, cfg Config) error {
	res, err := RunTimeout(ctx, cfg.Root, 5*time.Minute, nil, "docker", "compose", "-f", cfg.ComposeFile, "up", "-d", "--wait")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("docker compose up failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	return nil
}

func composeDown(ctx context.Context, cfg Config) error {
	res, err := RunTimeout(ctx, cfg.Root, 3*time.Minute, nil, "docker", "compose", "-f", cfg.ComposeFile, "down")
	if err != nil || res.ExitCode != 0 {
		if err == nil {
			err = fmt.Errorf("exit code %d", res.ExitCode)
		}
		return fmt.Errorf("docker compose down failed: %w\n%s", err, resultOutputTail(res, 8000))
	}
	return nil
}

func mergeSnapshots(a, b ComponentSnapshot) ComponentSnapshot {
	if a.DiskFreeBytes == 0 {
		a.DiskFreeBytes = b.DiskFreeBytes
	}
	a.E2EUp = b.E2EUp
	a.E2ERestarts = b.E2ERestarts
	a.Degraded = a.Degraded || b.Degraded
	return a
}

func nextBackoff(backoffs []time.Duration, idx *int) time.Duration {
	if len(backoffs) == 0 {
		return time.Second
	}
	if *idx >= len(backoffs) {
		return backoffs[len(backoffs)-1]
	}
	d := backoffs[*idx]
	*idx = *idx + 1
	return d
}

func sleepContext(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func pruneTimes(times []time.Time, cutoff time.Time) []time.Time {
	out := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			out = append(out, t)
		}
	}
	return out
}
