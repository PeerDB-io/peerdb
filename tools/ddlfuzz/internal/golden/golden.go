package golden

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/oracle"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/seed"
)

type Config struct {
	StateDir      string
	SeedsDir      string
	MySQLOracle   string
	MariaDBOracle string
	OracleTimeout time.Duration
	CaseDeadline  time.Duration
	Jobs          int
}

type Row struct {
	SQL       string `json:"sql"`
	Engine    string `json:"engine"`
	SQLMode   uint64 `json:"sql_mode"`
	Our       string `json:"our"`
	OurError  string `json:"our_error,omitempty"`
	Oracle    string `json:"oracle"`
	Expect    string `json:"expect,omitempty"`
	Class     string `json:"class,omitempty"`
	Reconcile bool   `json:"reconciled"`
}

type Summary struct {
	Checked        int `json:"checked"`
	Irreconcilable int `json:"irreconcilable"`
}

type job struct {
	index  int
	seed   seed.Seed
	engine string
}

type caseResult struct {
	row Row
	err error
}

func Run(ctx context.Context, cfg Config, w io.Writer) (Summary, error) {
	seeds, err := seed.LoadDir(cfg.SeedsDir)
	if err != nil {
		return Summary{}, err
	}
	if len(seeds) == 0 {
		return Summary{}, fmt.Errorf("no seeds loaded from %s", cfg.SeedsDir)
	}

	var jobs []job
	for _, s := range seeds {
		for _, engine := range expandEngine(s.Engine) {
			jobs = append(jobs, job{index: len(jobs), seed: s, engine: engine})
		}
	}

	workers := cfg.Jobs
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > len(jobs) {
		workers = len(jobs)
	}

	results := runPool(jobs, workers, func(j job) caseResult {
		row, err := runCase(ctx, cfg, j.seed, j.engine)
		return caseResult{row: row, err: err}
	})

	enc := json.NewEncoder(w)
	var summary Summary
	for i := range results {
		r := results[i]
		if r.err != nil {
			return summary, r.err
		}
		summary.Checked++
		if !r.row.Reconcile {
			summary.Irreconcilable++
			_ = enc.Encode(r.row)
		}
	}
	_ = enc.Encode(map[string]any{"summary": summary})
	return summary, nil
}

func runPool(jobs []job, workers int, fn func(job) caseResult) []caseResult {
	results := make([]caseResult, len(jobs))
	if len(jobs) == 0 {
		return results
	}
	if workers < 1 {
		workers = 1
	}
	jobCh := make(chan job)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				results[j.index] = fn(j)
			}
		}()
	}
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)
	wg.Wait()
	return results
}

func runCase(ctx context.Context, cfg Config, s seed.Seed, engine string) (Row, error) {
	engineID, _ := run.EngineID(engine)
	c := run.Case{SQL: []byte(s.SQL), SQLMode: s.SQLMode, Engine: engineID, Origin: run.OriginGolden}
	parser := ddllexec.NewWorker(0, cfg.CaseDeadline, nil)
	defer parser.Close()
	res := parser.RunBatch([]run.Case{c})[0]
	bin := cfg.MySQLOracle
	if engine == "mariadb" {
		bin = cfg.MariaDBOracle
	}
	d, _, err := oracle.SingleDigest(ctx, engine, bin, cfg.OracleTimeout, c, cfg.StateDir)
	if err != nil {
		return Row{}, err
	}
	div := compare.Diff(c, res.Sig, res.Err, res.Panic, d)
	oracleSig, _ := compare.OracleSigForEngine(d, c.Engine)
	row := Row{SQL: s.SQL, Engine: engine, SQLMode: s.SQLMode, Our: res.Sig, Oracle: oracleSig, Expect: s.ExpectSig, Reconcile: div == nil}
	if res.Err != nil {
		row.OurError = res.Err.Error()
	}
	if s.ExpectSig != "" && s.ExpectSig != "ERROR" && res.Sig != s.ExpectSig {
		row.Reconcile = false
		row.Class = "expect_mismatch"
	} else if s.ExpectSig == "ERROR" && res.Err == nil {
		row.Reconcile = false
		row.Class = "expect_error_missing"
	} else if div != nil {
		row.Class = div.Class
	}
	return row, nil
}

func expandEngine(engine string) []string {
	switch engine {
	case "mysql":
		return []string{"mysql"}
	case "mariadb", "maria":
		return []string{"mariadb"}
	default:
		return []string{"mysql", "mariadb"}
	}
}

func DefaultConfig(stateDir, seedsDir, mysqlOracle, mariaOracle string, timeout, caseDeadline time.Duration, jobs int) Config {
	return Config{
		StateDir:      stateDir,
		SeedsDir:      seedsDir,
		MySQLOracle:   filepath.Clean(mysqlOracle),
		MariaDBOracle: filepath.Clean(mariaOracle),
		OracleTimeout: timeout,
		CaseDeadline:  caseDeadline,
		Jobs:          jobs,
	}
}
