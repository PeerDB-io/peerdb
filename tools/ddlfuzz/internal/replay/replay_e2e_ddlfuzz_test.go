package replay

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/e2echeck"
)

func TestReplayE2EFindingOffline(t *testing.T) {
	stateDir := t.TempDir()
	before := e2echeck.Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
	}

	divAfter := e2echeck.Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"n1": {Name: "n1", Ordinal: 2, ColumnType: "bigint", IsNullable: "YES"},
	}
	divSig := "aaaaaaaaaaaa"
	writeE2EFinding(t, stateDir, divSig, "e2e-col-attr", "open", before, divAfter)

	var divOut bytes.Buffer
	if code := Run(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, divSig, &divOut); code != ExitDiverged {
		t.Fatalf("divergent replay exit = %d, want %d; out=%s", code, ExitDiverged, divOut.String())
	}
	var divRes Result
	if err := json.Unmarshal(divOut.Bytes(), &divRes); err != nil {
		t.Fatalf("decode divergent result: %v", err)
	}
	if divRes.Class != "e2e-col-attr" || divRes.Reconciled {
		t.Fatalf("divergent result = %+v", divRes)
	}

	okAfter := e2echeck.Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"n1": {Name: "n1", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
	}
	okSig := "bbbbbbbbbbbb"
	writeE2EFinding(t, stateDir, okSig, "e2e-col-attr", "open", before, okAfter)

	var okOut bytes.Buffer
	if code := Run(context.Background(), Config{StateDir: stateDir, CaseDeadline: time.Second}, okSig, &okOut); code != ExitOK {
		t.Fatalf("reconciled replay exit = %d, want %d; out=%s", code, ExitOK, okOut.String())
	}
	var okRes Result
	if err := json.Unmarshal(okOut.Bytes(), &okRes); err != nil {
		t.Fatalf("decode reconciled result: %v", err)
	}
	if !okRes.Reconciled || okRes.Class != "" {
		t.Fatalf("reconciled result = %+v", okRes)
	}
}

func TestRunAllE2EFindingOffline(t *testing.T) {
	stateDir := t.TempDir()
	before := e2echeck.Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
	}
	after := e2echeck.Snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"n1": {Name: "n1", Ordinal: 2, ColumnType: "bigint", IsNullable: "YES"},
	}
	writeE2EFinding(t, stateDir, "aaaaaaaaaaaa", "e2e-col-attr", "fixed", before, after)

	oldBatcher := newBatcher
	defer func() { newBatcher = oldBatcher }()
	newBatcher = func(engine, bin string, timeout time.Duration) digestBatcher {
		t.Fatalf("offline e2e finding used oracle batcher")
		return nil
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
	if len(summary.FixedRegressed) != 1 || summary.FixedRegressed[0] != "aaaaaaaaaaaa" {
		t.Fatalf("fixed_regressed = %#v", summary.FixedRegressed)
	}
	if len(summary.Regressions) != 1 || summary.Regressions[0].Class != "e2e-col-attr" {
		t.Fatalf("regressions = %+v", summary.Regressions)
	}
}

func writeE2EFinding(t *testing.T, stateDir, sig, class, status string, before, after e2echeck.Snapshot) {
	t.Helper()
	dir := filepath.Join(stateDir, "findings", sig)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	stmt := "ALTER TABLE t ADD COLUMN n1 INT"
	if err := os.WriteFile(filepath.Join(dir, "repro.sql"), []byte(stmt), 0o644); err != nil {
		t.Fatal(err)
	}
	meta := map[string]any{
		"sig":                 sig,
		"lane":                "e2e",
		"class":               class,
		"engine":              "mysql",
		"sql_mode":            0,
		"sql_mode_name":       "",
		"submitted_text":      stmt,
		"binlog_query":        stmt,
		"binlog_text_differs": false,
		"status_vars_hex":     replayStatusVarsHex(0),
		"info_schema_delta":   e2echeck.DiffSnapshots(before, after),
		"before_snapshot":     before,
		"after_snapshot":      after,
		"our_sig":             "",
		"our_error":           "",
		"server_image":        "mysql:9.7.0",
		"status":              status,
		"discovered_at":       "2026-07-03T00:00:00Z",
		"minimized":           false,
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func replayStatusVarsHex(mode uint64) string {
	var buf [14]byte
	buf[0] = 0
	buf[5] = 1
	binary.LittleEndian.PutUint64(buf[6:], mode)
	return hex.EncodeToString(buf[:])
}
