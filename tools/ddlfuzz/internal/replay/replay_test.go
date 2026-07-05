package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestOfflineE2EReproducible(t *testing.T) {
	complete := map[string]any{
		"lane":              "e2e",
		"class":             "e2e-col-attr",
		"submitted_text":    "ALTER TABLE t ADD COLUMN n1 INT",
		"binlog_query":      "ALTER TABLE t ADD COLUMN n1 INT",
		"status_vars_hex":   "00",
		"before_snapshot":   map[string]any{},
		"after_snapshot":    map[string]any{},
		"info_schema_delta": map[string]any{},
	}
	if !offlineE2EReproducible(complete) {
		t.Fatal("complete e2e capture meta should be offline-reproducible")
	}

	legacy := map[string]any{
		"lane":            "e2e",
		"class":           "e2e-position-missed",
		"submitted_text":  "ALTER TABLE t ADD COLUMN n1 INT",
		"status_vars_hex": "00",
	}
	if offlineE2EReproducible(legacy) {
		t.Fatal("pre-capture e2e meta must fall back to the fast lane")
	}

	oracleReject := map[string]any{
		"lane":  "e2e",
		"class": "oracle-reject-live-accept",
	}
	if offlineE2EReproducible(oracleReject) {
		t.Fatal("oracle-reject-live-accept must fall back to the fast lane")
	}

	fast := map[string]any{"lane": "fast", "class": "sig_mismatch"}
	if offlineE2EReproducible(fast) {
		t.Fatal("fast-lane finding must never take the e2e path")
	}
}

func TestRunAllBatchedVerdicts(t *testing.T) {
	stateDir := t.TempDir()
	writeReplayFinding(t, stateDir, "aaaaaaaaaaaa", "mysql", "fixed", "CREATE TABLE a (id INT)")
	writeReplayFinding(t, stateDir, "bbbbbbbbbbbb", "mysql", "fixed", "CREATE TABLE b (id INT)")
	writeReplayFinding(t, stateDir, "cccccccccccc", "mysql", "open", "CREATE TABLE c (id INT)")
	writeReplayFinding(t, stateDir, "dddddddddddd", "mysql", "ledgered", "CREATE TABLE d (id INT)")
	writeReplayFinding(t, stateDir, "eeeeeeeeeeee", "mysql", "parked", "CREATE TABLE e (id INT)")

	oldBatcher := newBatcher
	defer func() { newBatcher = oldBatcher }()
	newBatcher = func(engine, bin string, timeout time.Duration) digestBatcher {
		return &fakeBatcher{digests: []*digest.Digest{
			{Verdict: "reject"},
			acceptMismatchDigest(),
			acceptMismatchDigest(),
			{Verdict: "reject"},
			{Verdict: "reject"},
		}}
	}

	var out bytes.Buffer
	code := RunAll(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, &out)
	if code != ExitDiverged {
		t.Fatalf("RunAll exit = %d, want %d; out=%s", code, ExitDiverged, out.String())
	}
	var summary Summary
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Checked != 5 || summary.FixedOK != 1 || summary.Open != 1 || summary.Ledgered != 1 || summary.Parked != 1 {
		t.Fatalf("summary counts = %+v", summary)
	}
	if len(summary.FixedRegressed) != 1 || summary.FixedRegressed[0] != "bbbbbbbbbbbb" {
		t.Fatalf("fixed_regressed = %#v", summary.FixedRegressed)
	}
	if len(summary.Regressions) != 1 || summary.Regressions[0].Sig != "bbbbbbbbbbbb" || summary.Regressions[0].Class == "" || summary.Regressions[0].Shape == "" {
		t.Fatalf("regressions = %+v", summary.Regressions)
	}
}

func TestRunAllBatchedVerdictsMultipleOracleBatches(t *testing.T) {
	stateDir := t.TempDir()
	for i := range 300 {
		writeReplayFinding(t, stateDir, fmt.Sprintf("%012x", i), "mysql", "fixed", "CREATE TABLE t (id INT)")
	}

	oldBatcher := newBatcher
	defer func() { newBatcher = oldBatcher }()
	newBatcher = func(engine, bin string, timeout time.Duration) digestBatcher {
		return &fakeBatcher{}
	}

	var out bytes.Buffer
	code := RunAll(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, &out)
	if code != ExitOK {
		t.Fatalf("RunAll exit = %d, want %d; out=%s", code, ExitOK, out.String())
	}
	var summary Summary
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Checked != 300 || summary.FixedOK != 300 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestRunAllMalformed(t *testing.T) {
	stateDir := t.TempDir()
	dir := filepath.Join(stateDir, "findings", "aaaaaaaaaaaa")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "repro.sql"), []byte("CREATE TABLE t (id INT)"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	code := RunAll(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, &out)
	if code != ExitMalformed {
		t.Fatalf("RunAll exit = %d, want %d; out=%s", code, ExitMalformed, out.String())
	}
	var summary Summary
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if len(summary.Malformed) != 1 || summary.Malformed[0] != "aaaaaaaaaaaa" || summary.Checked != 0 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestRunAllBatchErrorFallsBackToReplayOne(t *testing.T) {
	stateDir := t.TempDir()
	writeReplayFinding(t, stateDir, "aaaaaaaaaaaa", "mysql", "fixed", "CREATE TABLE a (id INT)")

	oldBatcher := newBatcher
	oldSingle := singleDigest
	defer func() {
		newBatcher = oldBatcher
		singleDigest = oldSingle
	}()
	calls := 0
	newBatcher = func(engine, bin string, timeout time.Duration) digestBatcher {
		return &fakeBatcher{err: errors.New("batch failed")}
	}
	singleDigest = func(ctx context.Context, engine, binPath string, timeout time.Duration, c run.Case, stateDir string) (*digest.Digest, []byte, error) {
		calls++
		return &digest.Digest{Verdict: "reject"}, []byte(`{"verdict":"reject"}`), nil
	}

	var out bytes.Buffer
	code := RunAll(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, &out)
	if code != ExitOK {
		t.Fatalf("RunAll exit = %d, want %d; out=%s", code, ExitOK, out.String())
	}
	if calls != 1 {
		t.Fatalf("singleDigest calls = %d, want 1", calls)
	}
	var summary Summary
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.FixedOK != 1 || summary.Checked != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestRunAllOpenOracleCrashFallbackCountsOpen(t *testing.T) {
	stateDir := t.TempDir()
	writeReplayFindingWithClass(t, stateDir, "aaaaaaaaaaaa", "mysql", "open", "oracle_crash", "head=ALTER", "ALTER TABLE t ADD COLUMN n1 INT")

	oldBatcher := newBatcher
	oldSingle := singleDigest
	defer func() {
		newBatcher = oldBatcher
		singleDigest = oldSingle
	}()
	newBatcher = func(engine, bin string, timeout time.Duration) digestBatcher {
		return &fakeBatcher{err: errors.New("batch failed")}
	}
	singleDigest = func(context.Context, string, string, time.Duration, run.Case, string) (*digest.Digest, []byte, error) {
		return nil, nil, errors.New("EOF")
	}

	var out bytes.Buffer
	code := RunAll(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, &out)
	if code != ExitOK {
		t.Fatalf("RunAll exit = %d, want %d; out=%s", code, ExitOK, out.String())
	}
	var summary Summary
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Checked != 1 || summary.Open != 1 || len(summary.Malformed) != 0 {
		t.Fatalf("summary = %+v", summary)
	}
}

type fakeBatcher struct {
	digests []*digest.Digest
	err     error
}

func (f *fakeBatcher) Start(context.Context, string) error {
	return nil
}

func (f *fakeBatcher) ParseBatch(_ context.Context, cases []run.Case) ([]*digest.Digest, [][]byte, []uint32, error) {
	if f.err != nil {
		return nil, nil, nil, f.err
	}
	digests := f.digests
	if len(digests) == 0 {
		digests = make([]*digest.Digest, len(cases))
		for i := range digests {
			digests[i] = &digest.Digest{Verdict: "reject"}
		}
	}
	raw := make([][]byte, len(digests))
	for i, d := range digests {
		b, _ := json.Marshal(d)
		raw[i] = b
	}
	return digests, raw, make([]uint32, len(digests)), nil
}

func (f *fakeBatcher) Close() error {
	return nil
}

func acceptMismatchDigest() *digest.Digest {
	return &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
		Kind:   "alter_table",
		Schema: "",
		Table:  "oracle_only",
		Specs: []digest.Spec{{
			Op:   "add",
			Cols: []digest.Col{{Name: "oracle_col", TypeStr: "int"}},
		}},
	}}}
}

func writeReplayFinding(t *testing.T, stateDir, sig, engine, status, sql string) {
	writeReplayFindingWithClass(t, stateDir, sig, engine, status, "", "", sql)
}

func writeReplayFindingWithClass(t *testing.T, stateDir, sig, engine, status, class, shape, sql string) {
	t.Helper()
	dir := filepath.Join(stateDir, "findings", sig)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "repro.sql"), []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	meta := map[string]any{
		"sig":      sig,
		"engine":   engine,
		"sql_mode": 0,
		"status":   status,
	}
	if class != "" {
		meta["class"] = class
	}
	if shape != "" {
		meta["shape"] = shape
	}
	b, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}
