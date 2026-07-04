package findings

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRecordOpenRediscoveryPreservesOccurrenceMeta(t *testing.T) {
	state := t.TempDir()
	f := testFinding("first repro", "alter t{col a=int32}", "digest-1")
	sig, isNew, err := Record(state, f)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Fatalf("first record was not new")
	}
	dir := filepath.Join(state, "findings", sig)
	metaPath := filepath.Join(dir, "meta.json")
	firstMeta := readMetaMap(t, metaPath)
	firstLastSeen, _ := firstMeta["last_seen_at"].(string)

	f.Statement = []byte("second repro")
	f.OurSig = "alter t{col a=int64}"
	f.Meta["oracle_digest"] = "digest-2"
	f.Meta["submitted_text"] = "kept"
	_, isNew, err = Record(state, f)
	if err != nil {
		t.Fatal(err)
	}
	if isNew {
		t.Fatalf("rediscovery was reported new")
	}
	if got := string(mustRead(t, filepath.Join(dir, "repro.sql"))); got != "first repro" {
		t.Fatalf("repro was rewritten on open rediscovery: %q", got)
	}
	meta := readMetaMap(t, metaPath)
	if meta["our_sig"] != "alter t{col a=int32}" || meta["oracle_digest"] != "digest-1" || meta["shape"] != "stmt_count(alter≠rename)" {
		t.Fatalf("occurrence meta changed on open rediscovery: %#v", meta)
	}
	if intFromAny(meta["times_seen"]) != 2 {
		t.Fatalf("times_seen=%v, want 2", meta["times_seen"])
	}
	if meta["last_seen_at"] == "" || firstLastSeen == "" {
		t.Fatalf("last_seen_at missing before=%q after=%q", firstLastSeen, meta["last_seen_at"])
	}
	if meta["submitted_text"] != "original capture" {
		t.Fatalf("raw meta key was overwritten: %#v", meta)
	}
}

func TestRecordFixedReopenArchivesAndRewritesPairedMeta(t *testing.T) {
	state := t.TempDir()
	f := testFinding("first repro", "alter t{col a=int32}", "digest-1")
	sig, _, err := Record(state, f)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(state, "findings", sig)
	metaPath := filepath.Join(dir, "meta.json")
	setMetaStatus(t, metaPath, "fixed")

	f.Statement = []byte("second repro")
	f.OurSig = "alter t{col a=int64}"
	f.OurError = "new error"
	f.Meta["oracle_digest"] = "digest-2"
	f.Meta["submitted_text"] = "new capture"
	_, isNew, err := Record(state, f)
	if err != nil {
		t.Fatal(err)
	}
	if isNew {
		t.Fatalf("reopen should not append a new finding index")
	}
	if got := string(mustRead(t, filepath.Join(dir, "repro.prev1.sql"))); got != "first repro" {
		t.Fatalf("prev1=%q, want first repro", got)
	}
	if got := string(mustRead(t, filepath.Join(dir, "repro.sql"))); got != "second repro" {
		t.Fatalf("repro=%q, want second repro", got)
	}
	meta := readMetaMap(t, metaPath)
	if meta["status"] != "open" || intFromAny(meta["reopened_count"]) != 1 || meta["our_sig"] != "alter t{col a=int64}" || meta["oracle_digest"] != "digest-2" || meta["submitted_text"] != "new capture" {
		t.Fatalf("reopen meta mismatch: %#v", meta)
	}
}

func TestArchiveReproRingCapsAtThree(t *testing.T) {
	state := t.TempDir()
	f := testFinding("repro-0", "alter t{col a=int32}", "digest-0")
	sig, _, err := Record(state, f)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(state, "findings", sig)
	metaPath := filepath.Join(dir, "meta.json")
	for i := 1; i <= 4; i++ {
		setMetaStatus(t, metaPath, "fixed")
		f.Statement = []byte("repro-" + string(rune('0'+i)))
		f.OurSig = "alter t{col a=int32}"
		f.Meta["oracle_digest"] = "digest"
		if _, _, err := Record(state, f); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "repro.prev4.sql")); !os.IsNotExist(err) {
		t.Fatalf("prev4 exists or stat failed: %v", err)
	}
	for _, name := range []string{"repro.prev1.sql", "repro.prev2.sql", "repro.prev3.sql"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
	}
}

func testFinding(stmt, ourSig, digest string) Finding {
	return Finding{
		Class:     "sig_mismatch",
		Engine:    "mysql",
		Lane:      "fast",
		Statement: []byte(stmt),
		OurSig:    ourSig,
		Meta: map[string]any{
			"shape":          "stmt_count(alter≠rename)",
			"oracle_digest":  digest,
			"submitted_text": "original capture",
		},
	}
}

func readMetaMap(t *testing.T, path string) map[string]any {
	t.Helper()
	var meta map[string]any
	if err := json.Unmarshal(mustRead(t, path), &meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

func setMetaStatus(t *testing.T, path, status string) {
	t.Helper()
	meta := readMetaMap(t, path)
	meta["status"] = status
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
