package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/e2echeck"
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
	Regressions    []Result `json:"regressions,omitempty"`
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

type digestBatcher interface {
	Start(context.Context, string) error
	ParseBatch(context.Context, []run.Case) ([]*digest.Digest, [][]byte, error)
	Close() error
}

var (
	newBatcher = func(engine, bin string, timeout time.Duration) digestBatcher {
		return oracle.NewClient(engine, bin, timeout)
	}
	singleDigest = oracle.SingleDigest
)

func Run(ctx context.Context, cfg Config, sig string, w io.Writer) int {
	if sig != "" {
		res, code := replayOne(ctx, cfg, sig)
		_ = json.NewEncoder(w).Encode(res)
		return code
	}
	return RunAll(ctx, cfg, w)
}

type replayFinding struct {
	sig    string
	status string
	engine string
	mode   uint64
	sql    []byte
	c      run.Case
	result Result
	code   int
	done   bool
}

func RunAll(ctx context.Context, cfg Config, w io.Writer) int {
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
	var all []replayFinding
	byEngine := map[string][]int{"mysql": nil, "mariadb": nil}
	for _, ent := range entries {
		if !ent.IsDir() || len(ent.Name()) != 12 {
			continue
		}
		f := replayFinding{sig: ent.Name()}
		meta, sql, err := readFinding(cfg.StateDir, ent.Name())
		if err != nil {
			f.result = Result{Sig: ent.Name(), Class: "malformed", Shape: err.Error()}
			f.code = ExitMalformed
			f.done = true
			all = append(all, f)
			continue
		}
		f.status, _ = meta["status"].(string)
		if f.status == "" {
			f.status = "open"
		}
		f.engine, _ = meta["engine"].(string)
		if f.engine != "mysql" && f.engine != "mariadb" {
			f.result = Result{Sig: ent.Name(), Class: "malformed", Shape: "bad engine"}
			f.code = ExitMalformed
			f.done = true
			all = append(all, f)
			continue
		}
		f.mode, _ = uint64FromMeta(meta, "sql_mode")
		if offlineE2EReproducible(meta) {
			f.result, f.code = replayE2EOne(ent.Name(), f.engine, f.mode, meta)
			f.done = true
			all = append(all, f)
			continue
		}
		engineID, _ := run.EngineID(f.engine)
		f.sql = sql
		f.c = run.Case{SQL: sql, SQLMode: f.mode, Engine: engineID, Origin: run.OriginReplay}
		all = append(all, f)
		byEngine[f.engine] = append(byEngine[f.engine], len(all)-1)
	}
	for _, engine := range []string{"mysql", "mariadb"} {
		runReplayBatch(ctx, cfg, engine, byEngine[engine], all)
	}
	for _, f := range all {
		res, code := f.result, f.code
		if code == ExitMalformed {
			summary.Malformed = append(summary.Malformed, f.sig)
			continue
		}
		switch f.status {
		case "fixed":
			if res.Reconciled {
				summary.FixedOK++
			} else {
				summary.FixedRegressed = append(summary.FixedRegressed, f.sig)
				summary.Regressions = append(summary.Regressions, res)
				fmt.Fprintf(os.Stderr, "regressed %s: class=%s shape=%s ours=%s\n", f.sig, res.Class, res.Shape, resultOurs(res))
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

func runReplayBatch(ctx context.Context, cfg Config, engine string, indexes []int, all []replayFinding) {
	if len(indexes) == 0 {
		return
	}
	cases := make([]run.Case, len(indexes))
	for i, idx := range indexes {
		cases[i] = all[idx].c
	}
	parserResults := ddllexec.NewWorker(0, cfg.CaseDeadline, nil).RunBatch(cases)
	bin := cfg.MySQLOracle
	if engine == "mariadb" {
		bin = cfg.MariaDBOracle
	}
	var client digestBatcher
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()
	for start := 0; start < len(indexes); start += 256 {
		end := start + 256
		if end > len(indexes) {
			end = len(indexes)
		}
		if client == nil {
			client = newBatcher(engine, bin, cfg.Timeout)
			if err := client.Start(ctx, filepath.Join(cfg.StateDir, "log", "oracle-"+engine+"-replayall.log")); err != nil {
				_ = client.Close()
				client = nil
				fallbackReplayBatch(ctx, cfg, indexes[start:end], all)
				continue
			}
		}
		ds, raw, err := client.ParseBatch(ctx, cases[start:end])
		if err != nil || len(ds) != end-start || len(raw) != end-start {
			_ = client.Close()
			client = nil
			fallbackReplayBatch(ctx, cfg, indexes[start:end], all)
			continue
		}
		for i := start; i < end; i++ {
			idx := indexes[i]
			batchIdx := i - start
			all[idx].result, all[idx].code = replayFastResult(all[idx].sig, all[idx].engine, all[idx].mode, all[idx].c, parserResults[i], ds[batchIdx], raw[batchIdx])
			all[idx].done = true
		}
	}
}

func fallbackReplayBatch(ctx context.Context, cfg Config, indexes []int, all []replayFinding) {
	for _, idx := range indexes {
		all[idx].result, all[idx].code = replayOne(ctx, cfg, all[idx].sig)
		all[idx].done = true
	}
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
	if offlineE2EReproducible(meta) {
		return replayE2EOne(sig, engine, mode, meta)
	}
	engineID, _ := run.EngineID(engine)
	c := run.Case{SQL: sql, SQLMode: mode, Engine: engineID, Origin: run.OriginReplay}
	res := ddllexec.NewWorker(0, cfg.CaseDeadline, nil).RunBatch([]run.Case{c})[0]
	bin := cfg.MySQLOracle
	if engine == "mariadb" {
		bin = cfg.MariaDBOracle
	}
	d, raw, err := singleDigest(ctx, engine, bin, cfg.Timeout, c, cfg.StateDir)
	if err != nil {
		return Result{Sig: sig, Engine: engine, SQLMode: mode, Class: "malformed", Shape: err.Error()}, ExitMalformed
	}
	return replayFastResult(sig, engine, mode, c, res, d, raw)
}

func replayE2EOne(sig, engine string, mode uint64, meta map[string]any) (Result, int) {
	in, err := e2eInputFromMeta(meta)
	if err != nil {
		return Result{Sig: sig, Engine: engine, SQLMode: mode, Class: "malformed", Shape: err.Error(), OracleDigest: json.RawMessage("null")}, ExitMalformed
	}
	res, err := e2echeck.Reproduce(in)
	if err != nil {
		return Result{Sig: sig, Engine: engine, SQLMode: mode, OurSig: stringFromMeta(meta, "our_sig"), OurError: stringFromMeta(meta, "our_error"), Class: "malformed", Shape: err.Error(), OracleDigest: json.RawMessage("null")}, ExitMalformed
	}
	out := Result{
		Sig:          sig,
		Engine:       engine,
		SQLMode:      mode,
		OurSig:       stringFromMeta(meta, "our_sig"),
		OurError:     stringFromMeta(meta, "our_error"),
		OracleDigest: json.RawMessage("null"),
		Reconciled:   res.Reconciled,
	}
	if !res.Reconciled {
		out.Class = res.Class
		out.Shape = res.Shape
		return out, ExitDiverged
	}
	return out, ExitOK
}

func replayFastResult(sig, engine string, mode uint64, c run.Case, res ddllexec.Result, d *digest.Digest, raw []byte) (Result, int) {
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

func resultOurs(res Result) string {
	if res.OurError != "" {
		return res.OurError
	}
	return res.OurSig
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

// offlineE2EReproducible reports whether the finding carries the complete e2e
// capture meta the offline reproducer consumes. Findings recorded before the
// capture existed, and lane:"e2e" classes the reproducer does not model
// (oracle-reject-live-accept), keep replaying through the fast lane exactly as
// they did before the reproducer — this must not regress a running campaign's
// state (preflight treats exit 11 on any finding as blocking).
func offlineE2EReproducible(meta map[string]any) bool {
	if lane, _ := meta["lane"].(string); lane != "e2e" {
		return false
	}
	class, _ := meta["class"].(string)
	if !strings.HasPrefix(class, "e2e-") {
		return false
	}
	for _, key := range []string{
		"submitted_text", "binlog_query", "status_vars_hex",
		"before_snapshot", "after_snapshot", "info_schema_delta",
	} {
		if _, ok := meta[key]; !ok {
			return false
		}
	}
	return true
}

func e2eInputFromMeta(meta map[string]any) (e2echeck.Input, error) {
	engine, _ := meta["engine"].(string)
	class, _ := meta["class"].(string)
	mode, ok := uint64FromMeta(meta, "sql_mode")
	if !ok {
		return e2echeck.Input{}, fmt.Errorf("missing sql_mode")
	}
	submitted, ok := meta["submitted_text"].(string)
	if !ok || submitted == "" {
		return e2echeck.Input{}, fmt.Errorf("missing submitted_text")
	}
	binlogQuery, ok := meta["binlog_query"].(string)
	if !ok || binlogQuery == "" {
		return e2echeck.Input{}, fmt.Errorf("missing binlog_query")
	}
	statusVarsHex, ok := meta["status_vars_hex"].(string)
	if !ok {
		return e2echeck.Input{}, fmt.Errorf("missing status_vars_hex")
	}
	before, err := decodeMetaValue[e2echeck.Snapshot](meta, "before_snapshot")
	if err != nil {
		return e2echeck.Input{}, err
	}
	after, err := decodeMetaValue[e2echeck.Snapshot](meta, "after_snapshot")
	if err != nil {
		return e2echeck.Input{}, err
	}
	delta, err := decodeMetaValue[e2echeck.Delta](meta, "info_schema_delta")
	if err != nil {
		return e2echeck.Input{}, err
	}
	in := e2echeck.Input{
		Engine:        engine,
		IsMariaDB:     engine == "mariadb",
		SQLMode:       mode,
		Submitted:     submitted,
		BinlogQuery:   binlogQuery,
		StatusVarsHex: statusVarsHex,
		Before:        before,
		After:         after,
		Delta:         delta,
		Class:         class,
	}
	if expected, ok := uint64FromMeta(meta, "expected_relevant"); ok {
		in.ExpectedRelevant = &expected
	}
	return in, nil
}

func decodeMetaValue[T any](meta map[string]any, key string) (T, error) {
	var out T
	raw, ok := meta[key]
	if !ok || raw == nil {
		return out, fmt.Errorf("missing %s", key)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return out, fmt.Errorf("decode %s: %w", key, err)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("decode %s: %w", key, err)
	}
	return out, nil
}

func uint64FromMeta(meta map[string]any, key string) (uint64, bool) {
	switch v := meta[key].(type) {
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint64:
		return v, true
	case json.Number:
		u, err := v.Int64()
		if err != nil || u < 0 {
			return 0, false
		}
		return uint64(u), true
	default:
		return 0, false
	}
}

func stringFromMeta(meta map[string]any, key string) string {
	s, _ := meta[key].(string)
	return s
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

func ErrMissingOracle(path string) error {
	return fmt.Errorf("oracle binary not found: %s", path)
}
