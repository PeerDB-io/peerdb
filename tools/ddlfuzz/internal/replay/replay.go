package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/findings"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/oracle"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

const (
	ExitOK        = 0
	ExitDiverged  = 10
	ExitMalformed = 11
	ExitInternal  = 1
)

type Config struct {
	StateDir      string
	MySQLOracle   string
	MariaDBOracle string
	Timeout       time.Duration
	CaseDeadline  time.Duration
}

type Summary struct {
	Checked        int      `json:"checked"`
	FixedOK        int      `json:"fixed_ok"`
	FixedRegressed []string `json:"fixed_regressed"`
	Malformed      []string `json:"malformed"`
	Open           int      `json:"open"`
	Ledgered       int      `json:"ledgered"`
	Parked         int      `json:"parked"`
}

type Result struct {
	Sig          string          `json:"sig"`
	Engine       string          `json:"engine"`
	SQLMode      uint64          `json:"sql_mode"`
	OurSig       string          `json:"our_sig"`
	OurError     string          `json:"our_error,omitempty"`
	OracleDigest json.RawMessage `json:"oracle_digest,omitempty"`
	Reconciled   bool            `json:"reconciled"`
	Class        string          `json:"class,omitempty"`
	Shape        string          `json:"shape,omitempty"`
}

func Run(ctx context.Context, cfg Config, sig string, w io.Writer) int {
	if sig != "" {
		res, code := replayOne(ctx, cfg, sig)
		_ = json.NewEncoder(w).Encode(res)
		return code
	}
	summary := Summary{}
	root := filepath.Join(cfg.StateDir, "findings")
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		_ = json.NewEncoder(w).Encode(summary)
		return ExitOK
	}
	if err != nil {
		summary.Malformed = append(summary.Malformed, root)
		_ = json.NewEncoder(w).Encode(summary)
		return ExitMalformed
	}
	for _, ent := range entries {
		if !ent.IsDir() || len(ent.Name()) != 12 {
			continue
		}
		res, code := replayOne(ctx, cfg, ent.Name())
		if code == ExitMalformed {
			summary.Malformed = append(summary.Malformed, ent.Name())
			continue
		}
		status := statusOf(cfg.StateDir, ent.Name())
		switch status {
		case "fixed":
			if res.Reconciled {
				summary.FixedOK++
			} else {
				summary.FixedRegressed = append(summary.FixedRegressed, ent.Name())
			}
		case "ledgered":
			summary.Ledgered++
		case "parked":
			summary.Parked++
		default:
			summary.Open++
		}
		summary.Checked++
	}
	_ = json.NewEncoder(w).Encode(summary)
	if len(summary.Malformed) > 0 {
		return ExitMalformed
	}
	if len(summary.FixedRegressed) > 0 {
		return ExitDiverged
	}
	return ExitOK
}

func replayOne(ctx context.Context, cfg Config, sig string) (Result, int) {
	meta, sql, err := readFinding(cfg.StateDir, sig)
	if err != nil {
		return Result{Sig: sig, Class: "malformed", Shape: err.Error()}, ExitMalformed
	}
	engine, _ := meta["engine"].(string)
	if engine != "mysql" && engine != "mariadb" {
		return Result{Sig: sig, Class: "malformed", Shape: "bad engine"}, ExitMalformed
	}
	mode := uint64(0)
	if f, ok := meta["sql_mode"].(float64); ok {
		mode = uint64(f)
	}
	engineID, _ := run.EngineID(engine)
	c := run.Case{SQL: sql, SQLMode: mode, Engine: engineID, Origin: run.OriginReplay}
	res := ddllexec.NewWorker(0, cfg.CaseDeadline, nil).RunBatch([]run.Case{c})[0]
	bin := cfg.MySQLOracle
	if engine == "mariadb" {
		bin = cfg.MariaDBOracle
	}
	d, raw, err := oracle.SingleDigest(ctx, engine, bin, cfg.Timeout, c, cfg.StateDir)
	if err != nil {
		return Result{Sig: sig, Engine: engine, SQLMode: mode, Class: "malformed", Shape: err.Error()}, ExitMalformed
	}
	div := compare.Diff(c, res.Sig, res.Err, res.Panic, d)
	out := Result{Sig: sig, Engine: engine, SQLMode: mode, OurSig: res.Sig, OracleDigest: raw, Reconciled: div == nil}
	if res.Err != nil {
		out.OurError = res.Err.Error()
	}
	if div != nil {
		out.Class = div.Class
		out.Shape = div.Shape
		return out, ExitDiverged
	}
	return out, ExitOK
}

func RunExpectAccept(ctx context.Context, cfg Config, from string, w io.Writer) int {
	f, err := os.Open(from)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return ExitInternal
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	filed := 0
	line := 0
	for sc.Scan() {
		line++
		text := sc.Bytes()
		if len(text) == 0 {
			continue
		}
		var row struct {
			Engine    string `json:"engine"`
			SQLMode   uint64 `json:"sql_mode"`
			Statement string `json:"statement"`
		}
		if err := json.Unmarshal(text, &row); err != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "line": line})
			return ExitInternal
		}
		engineID, err := run.EngineID(row.Engine)
		if err != nil {
			return ExitInternal
		}
		c := run.Case{SQL: []byte(row.Statement), SQLMode: row.SQLMode, Engine: engineID, Origin: run.OriginReplay}
		bin := cfg.MySQLOracle
		if row.Engine == "mariadb" {
			bin = cfg.MariaDBOracle
		}
		d, raw, err := oracle.SingleDigest(ctx, row.Engine, bin, cfg.Timeout, c, cfg.StateDir)
		if err != nil {
			return ExitInternal
		}
		if d.Verdict == "reject" {
			_, _, _ = findings.Record(cfg.StateDir, findings.Finding{
				Class:     "oracle-reject-live-accept",
				Engine:    row.Engine,
				SQLMode:   row.SQLMode,
				Lane:      "e2e",
				Statement: c.SQL,
				Meta: map[string]any{
					"shape":         "head=" + compare.HeadWord(c.SQL),
					"oracle_error":  d.Error,
					"source_line":   line,
					"oracle_digest": raw,
				},
			})
			filed++
		}
	}
	if err := sc.Err(); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "line": line})
		return ExitInternal
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"checked": line, "findings": filed})
	if filed > 0 {
		return ExitDiverged
	}
	return ExitOK
}

func readFinding(stateDir, sig string) (map[string]any, []byte, error) {
	dir := filepath.Join(stateDir, "findings", sig)
	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, nil, err
	}
	var meta map[string]any
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, nil, err
	}
	sql, err := os.ReadFile(filepath.Join(dir, "repro.sql"))
	if err != nil {
		return nil, nil, err
	}
	return meta, sql, nil
}

func statusOf(stateDir, sig string) string {
	b, err := os.ReadFile(filepath.Join(stateDir, "findings", sig, "meta.json"))
	if err != nil {
		return "malformed"
	}
	var meta map[string]any
	if err := json.Unmarshal(b, &meta); err != nil {
		return "malformed"
	}
	s, _ := meta["status"].(string)
	if s == "" {
		s = "open"
	}
	return s
}

func ErrMissingOracle(path string) error {
	return fmt.Errorf("oracle binary not found: %s", path)
}
