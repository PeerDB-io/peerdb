package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/golden"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/minimize"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/replay"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

type config struct {
	stateDir             string
	mysqlOracle          string
	mariaOracle          string
	oracleProcsPerEngine int
	oracleBatchTimeout   time.Duration
	batch                int
	execWorkers          int
	genWorkers           int
	caseDeadline         time.Duration
	memCeiling           int64
	seed                 uint64
	seedsDir             string
	engineBias           float64
	mutRatio             float64
	statsInterval        time.Duration
	minimizeBudget       time.Duration
	maxOpenFindings      int
	duration             time.Duration
	corpusBudget         int64
	retainPerPoll        int
}

func defaultConfig() config {
	execWorkers := runtime.GOMAXPROCS(0) - 2
	if execWorkers < 1 {
		execWorkers = 1
	}
	baseDir := defaultBaseDir()
	return config{
		stateDir:             filepath.Join(baseDir, "state"),
		mysqlOracle:          filepath.Join(baseDir, "build", "oracle-mysql"),
		mariaOracle:          filepath.Join(baseDir, "build", "oracle-mariadb"),
		oracleProcsPerEngine: 5,
		oracleBatchTimeout:   10 * time.Second,
		batch:                1000,
		execWorkers:          execWorkers,
		genWorkers:           2,
		caseDeadline:         100 * time.Millisecond,
		memCeiling:           4 << 30,
		seedsDir:             filepath.Join(baseDir, "seeds"),
		engineBias:           0.5,
		mutRatio:             0.6,
		statsInterval:        5 * time.Second,
		minimizeBudget:       30 * time.Second,
		maxOpenFindings:      200,
		corpusBudget:         40 << 30,
		retainPerPoll:        256,
	}
}

func defaultBaseDir() string {
	if _, err := os.Stat(filepath.Join("tools", "ddlfuzz", "go.mod")); err == nil {
		return filepath.Join("tools", "ddlfuzz")
	}
	return "."
}

func addCommonFlags(fs *flag.FlagSet, cfg *config) {
	fs.StringVar(&cfg.stateDir, "state", cfg.stateDir, "state directory")
	fs.StringVar(&cfg.mysqlOracle, "mysql-oracle", cfg.mysqlOracle, "MySQL oracle path")
	fs.StringVar(&cfg.mariaOracle, "maria-oracle", cfg.mariaOracle, "MariaDB oracle path")
	fs.IntVar(&cfg.oracleProcsPerEngine, "oracle-procs-per-engine", cfg.oracleProcsPerEngine, "oracle processes per engine")
	fs.DurationVar(&cfg.oracleBatchTimeout, "oracle-batch-timeout", cfg.oracleBatchTimeout, "oracle batch timeout")
	fs.IntVar(&cfg.batch, "batch", cfg.batch, "cases per oracle batch")
	fs.IntVar(&cfg.execWorkers, "exec-workers", cfg.execWorkers, "parser execution workers")
	fs.IntVar(&cfg.genWorkers, "gen-workers", cfg.genWorkers, "generator workers")
	fs.DurationVar(&cfg.caseDeadline, "case-deadline", cfg.caseDeadline, "per-case parser deadline")
	fs.Int64Var(&cfg.memCeiling, "mem-ceiling", cfg.memCeiling, "memory ceiling in bytes")
	fs.Uint64Var(&cfg.seed, "seed", cfg.seed, "PRNG seed; 0 uses a time-based seed")
	fs.StringVar(&cfg.seedsDir, "seeds", cfg.seedsDir, "seed corpus directory")
	fs.Float64Var(&cfg.engineBias, "engine-bias", cfg.engineBias, "probability of choosing MySQL")
	fs.Float64Var(&cfg.mutRatio, "mut-ratio", cfg.mutRatio, "mutation ratio")
	fs.DurationVar(&cfg.statsInterval, "stats-interval", cfg.statsInterval, "stderr stats interval")
	fs.DurationVar(&cfg.minimizeBudget, "minimize-budget", cfg.minimizeBudget, "minimization time budget")
	fs.IntVar(&cfg.maxOpenFindings, "max-open-findings", cfg.maxOpenFindings, "maximum open findings to record")
	fs.DurationVar(&cfg.duration, "duration", cfg.duration, "fuzzing duration; 0 runs until signal")
	fs.Int64Var(&cfg.corpusBudget, "corpus-budget", cfg.corpusBudget, "corpus SQLite disk budget in bytes; 0 = unlimited")
	fs.IntVar(&cfg.retainPerPoll, "retain-per-poll", cfg.retainPerPoll, "max corpus inputs retained per coverage-growth poll; 0 = unlimited")
}

func main() {
	cfg := defaultConfig()
	root := flag.NewFlagSet("ddlfuzz", flag.ExitOnError)
	root.SetOutput(os.Stderr)
	addCommonFlags(root, &cfg)
	root.Usage = func() {
		usage(os.Stderr, root)
	}

	if len(os.Args) == 1 {
		root.Usage()
		os.Exit(2)
	}
	if err := root.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}

	args := root.Args()
	if len(args) == 0 {
		root.Usage()
		os.Exit(2)
	}

	cmd := args[0]
	switch cmd {
	case "fuzz", "golden", "replay", "minimize":
		os.Exit(runCommand(cmd, cfg, args[1:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		root.Usage()
		os.Exit(2)
	}
}

func runCommand(cmd string, cfg config, args []string) int {
	fs := flag.NewFlagSet("ddlfuzz "+cmd, flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	addCommonFlags(fs, &cfg)
	var from string
	var expectAccept bool
	if cmd == "replay" {
		fs.StringVar(&from, "from", "", "JSONL input for oracle cross-check replay")
		fs.BoolVar(&expectAccept, "expect-accept", false, "expect replay --from inputs to be accepted by the oracle")
	}
	fs.Usage = func() {
		commandUsage(os.Stderr, fs, cmd)
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch cmd {
	case "fuzz":
		return runFuzz(ctx, cfg)
	case "golden":
		if err := requireOracles(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "golden deferred: %v\n", err)
			return 1
		}
		summary, err := golden.Run(ctx, golden.DefaultConfig(cfg.stateDir, cfg.seedsDir, cfg.mysqlOracle, cfg.mariaOracle, cfg.oracleBatchTimeout, cfg.caseDeadline), os.Stdout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "golden: %v\n", err)
			return 1
		}
		if summary.Irreconcilable > 0 {
			return 1
		}
		return 0
	case "replay":
		rcfg := replay.Config{StateDir: cfg.stateDir, MySQLOracle: cfg.mysqlOracle, MariaDBOracle: cfg.mariaOracle, Timeout: cfg.oracleBatchTimeout, CaseDeadline: cfg.caseDeadline}
		if from != "" {
			if !expectAccept {
				fmt.Fprintln(os.Stderr, "replay --from requires --expect-accept")
				return replay.ExitInternal
			}
			return replay.RunExpectAccept(ctx, rcfg, from, os.Stdout)
		}
		rest := fs.Args()
		sig := ""
		if len(rest) > 1 {
			fs.Usage()
			return 2
		}
		if len(rest) == 1 {
			sig = rest[0]
		}
		return replay.Run(ctx, rcfg, sig, os.Stdout)
	case "minimize":
		rest := fs.Args()
		if len(rest) != 1 {
			fs.Usage()
			return 2
		}
		return runMinimize(cfg, rest[0])
	default:
		return 2
	}
}

func usage(w io.Writer, fs *flag.FlagSet) {
	fmt.Fprintln(w, "Usage: ddlfuzz [flags] fuzz|golden|replay|minimize [args]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Flags:")
	fs.PrintDefaults()
}

func commandUsage(w io.Writer, fs *flag.FlagSet, cmd string) {
	fmt.Fprintf(w, "Usage: ddlfuzz %s [flags] [args]\n", cmd)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Flags:")
	fs.PrintDefaults()
}

func requireOracles(cfg config) error {
	for _, path := range []string{cfg.mysqlOracle, cfg.mariaOracle} {
		if _, err := os.Stat(path); err != nil {
			return err
		}
	}
	return nil
}

func runFuzz(ctx context.Context, cfg config) int {
	if cfg.memCeiling > 0 {
		debug.SetMemoryLimit(cfg.memCeiling * 3 / 4)
	}
	return runFuzzLoop(ctx, cfg)
}

func runMinimize(cfg config, sig string) int {
	dir := filepath.Join(cfg.stateDir, "findings", sig)
	sql, err := os.ReadFile(filepath.Join(dir, "repro.sql"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "minimize: %v\n", err)
		return 11
	}
	minimize.Budget = cfg.minimizeBudget
	out := minimize.Minimize(sql, 0, "", minimize.FastLanePredicate(sig))
	if len(out) < len(sql) {
		if err := os.WriteFile(filepath.Join(dir, "repro.sql"), out, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "minimize: %v\n", err)
			return 1
		}
	}
	fmt.Fprintf(os.Stdout, `{"sig":%q,"before":%d,"after":%d}`+"\n", sig, len(sql), len(out))
	return 0
}

func loadOrInitSeed(cfg config) (uint64, error) {
	if cfg.seed != 0 {
		if err := os.WriteFile(filepath.Join(cfg.stateDir, "seed"), []byte(strconv.FormatUint(cfg.seed, 10)+"\n"), 0o644); err != nil {
			return 0, err
		}
		return cfg.seed, nil
	}
	b, err := os.ReadFile(filepath.Join(cfg.stateDir, "seed"))
	if err == nil {
		v, err := strconv.ParseUint(string(bytesTrimSpace(b)), 10, 64)
		if err == nil && v != 0 {
			return v, nil
		}
	}
	v := uint64(time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(cfg.stateDir, "seed"), []byte(strconv.FormatUint(v, 10)+"\n"), 0o644); err != nil {
		return 0, err
	}
	return v, nil
}

func chooseMode(rng *rand.Rand, engine uint8) uint64 {
	modes := []uint64{0, 0, 0, 1 << 2, 1 << 20, (1 << 2) | (1 << 20)}
	if engine == run.EngineMariaDB {
		modes = append(modes, 1<<9, 1<<10, (1<<9)|(1<<2), (1<<10)|(1<<20))
	}
	return modes[rng.IntN(len(modes))]
}

func writeStatsJSON(stateDir string, v map[string]any) error {
	b, _ := json.MarshalIndent(v, "", "  ")
	b = append(b, '\n')
	path := filepath.Join(stateDir, "stats.json")
	tmp, err := os.CreateTemp(filepath.Dir(path), ".stats.json.tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

func bytesTrimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}
