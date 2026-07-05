package fuzzcmd

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/golden"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/replay"
)

type config struct {
	stateDir             string
	mysqlOracle          string
	mariaOracle          string
	oracleProcsPerEngine int
	oracleBatchTimeout   time.Duration
	batch                int
	genWorkers           int
	caseDeadline         time.Duration
	memCeiling           int64
	seed                 uint64
	seedsDir             string
	engineBias           float64
	mutRatio             float64
	statsInterval        time.Duration
	maxOpenFindings      int
	duration             time.Duration
	corpusBudget         int64
	retainPerBatch       int
	behaviorSince        string
	behaviorUntil        string
}

func defaultConfig() config {
	baseDir := defaultBaseDir()
	return config{
		stateDir:             filepath.Join(baseDir, "state"),
		mysqlOracle:          filepath.Join(baseDir, "build", "oracle-mysql"),
		mariaOracle:          filepath.Join(baseDir, "build", "oracle-mariadb"),
		oracleProcsPerEngine: 5,
		oracleBatchTimeout:   10 * time.Second,
		batch:                1000,
		genWorkers:           2,
		caseDeadline:         100 * time.Millisecond,
		memCeiling:           4 << 30,
		seedsDir:             filepath.Join(baseDir, "seeds"),
		engineBias:           0.5,
		mutRatio:             0.6,
		statsInterval:        5 * time.Second,
		maxOpenFindings:      200,
		corpusBudget:         40 << 30,
		retainPerBatch:       256,
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
	fs.IntVar(&cfg.genWorkers, "gen-workers", cfg.genWorkers, "generator workers")
	fs.DurationVar(&cfg.caseDeadline, "case-deadline", cfg.caseDeadline, "per-case parser deadline")
	fs.Int64Var(&cfg.memCeiling, "mem-ceiling", cfg.memCeiling, "memory ceiling in bytes")
	fs.Uint64Var(&cfg.seed, "seed", cfg.seed, "PRNG seed; 0 uses a time-based seed")
	fs.StringVar(&cfg.seedsDir, "seeds", cfg.seedsDir, "seed corpus directory")
	fs.Float64Var(&cfg.engineBias, "engine-bias", cfg.engineBias, "probability of choosing MySQL")
	fs.Float64Var(&cfg.mutRatio, "mut-ratio", cfg.mutRatio, "mutation ratio")
	fs.DurationVar(&cfg.statsInterval, "stats-interval", cfg.statsInterval, "stderr stats interval")
	fs.IntVar(&cfg.maxOpenFindings, "max-open-findings", cfg.maxOpenFindings, "maximum open findings to record")
	fs.DurationVar(&cfg.duration, "duration", cfg.duration, "fuzzing duration; 0 runs until signal")
	fs.Int64Var(&cfg.corpusBudget, "corpus-budget", cfg.corpusBudget, "corpus SQLite disk budget in bytes; 0 = unlimited")
	fs.IntVar(&cfg.retainPerBatch, "retain-per-batch", cfg.retainPerBatch, "max corpus inputs retained per batch on fresh oracle edges, smallest first")
}

func Main() {
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
	case "fuzz", "golden", "replay", "corpus-migrate", "corpus-distill":
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
	var replayAll bool
	goldenJobs := runtime.GOMAXPROCS(0)
	if cmd == "replay" {
		fs.StringVar(&from, "from", "", "JSONL input for oracle cross-check replay")
		fs.BoolVar(&expectAccept, "expect-accept", false, "expect replay --from inputs to be accepted by the oracle")
		fs.BoolVar(&replayAll, "all", false, "replay all findings")
	}
	if cmd == "golden" {
		fs.IntVar(&goldenJobs, "jobs", goldenJobs, "golden worker-pool size (parallel cases); default GOMAXPROCS; 1 = sequential")
	}
	if cmd == "corpus-distill" {
		fs.StringVar(&cfg.behaviorSince, "behavior-since", cfg.behaviorSince, "RFC3339 start time for one-off behavior-tier distill window")
		fs.StringVar(&cfg.behaviorUntil, "behavior-until", cfg.behaviorUntil, "RFC3339 end time for one-off behavior-tier distill window")
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
		summary, err := golden.Run(ctx, golden.DefaultConfig(cfg.stateDir, cfg.seedsDir, cfg.mysqlOracle, cfg.mariaOracle, cfg.oracleBatchTimeout, cfg.caseDeadline, goldenJobs), os.Stdout)
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
		if replayAll && len(rest) != 0 {
			fs.Usage()
			return 2
		}
		if replayAll {
			return replay.RunAll(ctx, rcfg, os.Stdout)
		}
		if len(rest) == 1 {
			sig = rest[0]
		}
		return replay.Run(ctx, rcfg, sig, os.Stdout)
	case "corpus-migrate":
		rest := fs.Args()
		if len(rest) != 0 {
			fs.Usage()
			return 2
		}
		return runCorpusMigrate(cfg)
	case "corpus-distill":
		rest := fs.Args()
		if len(rest) != 0 {
			fs.Usage()
			return 2
		}
		return runCorpusDistill(cfg)
	default:
		return 2
	}
}

func usage(w io.Writer, fs *flag.FlagSet) {
	fmt.Fprintln(w, "Usage: ddlfuzz [flags] fuzz|golden|replay|corpus-migrate|corpus-distill [args]")
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

func runCorpusMigrate(cfg config) int {
	store, err := corpus.Open(filepath.Join(cfg.stateDir, "corpus.db"), cfg.corpusBudget)
	if err != nil {
		fmt.Fprintf(os.Stderr, "corpus-migrate: %v\n", err)
		return 1
	}
	defer store.Close()
	if err := store.StampExistingNoise(); err != nil {
		fmt.Fprintf(os.Stderr, "corpus-migrate: %v\n", err)
		return 1
	}
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
