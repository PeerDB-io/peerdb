package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	ddl := filepath.Join(root, "tools", "ddlfuzz")
	cfg := Config{
		Root:        root,
		DDLDir:      ddl,
		StateDir:    filepath.Join(ddl, "state"),
		BuildDir:    filepath.Join(ddl, "build"),
		PromptPath:  filepath.Join(ddl, "supervisor", "prompt.tmpl"),
		DDLfuzzBin:  filepath.Join(ddl, "build", "ddlfuzz"),
		E2EBin:      filepath.Join(ddl, "build", "ddlfuzz-e2e"),
		MySQLOracle: filepath.Join(ddl, "build", "oracle-mysql"),
		MariaOracle: filepath.Join(ddl, "build", "oracle-mariadb"),
		StartedAt:   time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC),
		Deadline:    time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC),
	}
	if err := cfg.ensureStateDirs(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestGroupKeyNormalization(t *testing.T) {
	a := FindingMeta{Class: "error-diverge", OurError: "Unknown column 'Customer42' near `db9` at line 8848"}
	b := FindingMeta{Class: "error-diverge", OurError: "unknown column 'Order17' near `prod` at line 12"}
	ga := GroupInfoForMeta(a)
	gb := GroupInfoForMeta(b)
	if ga.Key != gb.Key {
		t.Fatalf("normalized group keys differ: %s vs %s; shapes %q %q", ga.Key, gb.Key, ga.Shape, gb.Shape)
	}
	c := FindingMeta{Class: "panic", OurError: a.OurError}
	if GroupInfoForMeta(c).Key == ga.Key {
		t.Fatalf("different classes should not collapse to the same group key")
	}
}

func TestBudgetArithmetic(t *testing.T) {
	base := time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
	records := []AttemptRecord{
		{Attempt: 1, StartedAt: base, EndedAt: base.Add(45 * time.Minute)},
		{Attempt: 2, StartedAt: base.Add(time.Hour), EndedAt: base.Add(2 * time.Hour)},
	}
	if BudgetExhausted(records, 3, 150*time.Minute) {
		t.Fatalf("budget exhausted too early")
	}
	records = append(records, AttemptRecord{Attempt: 3, StartedAt: base.Add(3 * time.Hour), EndedAt: base.Add(3*time.Hour + time.Minute)})
	if !BudgetExhausted(records, 3, 150*time.Minute) {
		t.Fatalf("third attempt should exhaust budget")
	}
	records = []AttemptRecord{{Attempt: 1, StartedAt: base, EndedAt: base.Add(151 * time.Minute)}}
	if !BudgetExhausted(records, 3, 150*time.Minute) {
		t.Fatalf("wall time should exhaust budget")
	}
}

func TestTouchesE2EBinary(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		want  bool
	}{
		{name: "flow parser", paths: []string{"flow/connectors/mysql/ddl_parser.go"}, want: true},
		{name: "internal e2echeck", paths: []string{"tools/ddlfuzz/internal/e2echeck/compare.go"}, want: true},
		{name: "e2e harness", paths: []string{"tools/ddlfuzz/e2e/matcher.go"}, want: true},
		{name: "e2e main", paths: []string{"tools/ddlfuzz/cmd/ddlfuzz-e2e/main.go"}, want: true},
		{name: "go mod", paths: []string{"tools/ddlfuzz/go.mod"}, want: true},
		{name: "oracle", paths: []string{"tools/ddlfuzz/oracle/mysql/digest.cc"}},
		{name: "seeds", paths: []string{"tools/ddlfuzz/seeds/x.sql"}},
		{name: "supervisor", paths: []string{"tools/ddlfuzz/supervisor/fixloop.go"}},
		{name: "docs", paths: []string{"docs/x.md"}},
		{name: "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := touchesE2EBinary(tt.paths); got != tt.want {
				t.Fatalf("touchesE2EBinary(%v)=%v, want %v", tt.paths, got, tt.want)
			}
		})
	}
}

func TestE2EHotRestartLaneDown(t *testing.T) {
	cfg := testConfig(t)
	if err := os.MkdirAll(cfg.BuildDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oldBin := []byte("old")
	newBin := []byte("new")
	mustWrite(t, cfg.E2EBin, string(oldBin))
	newPath := filepath.Join(cfg.BuildDir, "ddlfuzz-e2e.new")
	mustWrite(t, newPath, string(newBin))

	m := NewE2EManager(cfg, nil)
	if err := m.HotRestart(context.Background(), newPath); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(cfg.E2EBin)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(newBin) {
		t.Fatalf("renamed binary mismatch: %q", got)
	}
	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf("new binary still exists or stat failed: %v", err)
	}
}

func TestPriorFixEvidence(t *testing.T) {
	tests := []struct {
		name    string
		meta    FindingMeta
		records []AttemptRecord
		want    bool
	}{
		{name: "fixed by sibling", meta: FindingMeta{FixedBy: "aaaaaaaaaaaa"}, want: true},
		{name: "prior fixed outcome", records: []AttemptRecord{{Outcome: "failed"}, {Outcome: "fixed"}}, want: true},
		{name: "none", records: []AttemptRecord{{Outcome: "did-not-reproduce"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := priorFixEvidence(tt.meta, tt.records); got != tt.want {
				t.Fatalf("priorFixEvidence()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfirmFixed(t *testing.T) {
	t.Run("fixed by exits zero", func(t *testing.T) {
		cfg := testConfig(t)
		writeReplayStub(t, cfg, 0)
		writeMetaFixture(t, cfg, "aaaaaaaaaaaa", "open")
		metaPath := filepath.Join(cfg.StateDir, "findings", "aaaaaaaaaaaa", "meta.json")
		meta, err := loadFindingMeta(metaPath)
		if err != nil {
			t.Fatal(err)
		}
		meta.FixedBy = "bbbbbbbbbbbb"
		if err := writeFindingMeta(metaPath, meta); err != nil {
			t.Fatal(err)
		}
		attemptPath := filepath.Join(cfg.StateDir, "attempts", "aaaaaaaaaaaa.jsonl")

		finding := mustFinding(t, cfg, "aaaaaaaaaaaa")
		if !priorFixEvidence(finding.Meta, nil) || !confirmFixed(context.Background(), cfg, finding, nil) {
			t.Fatalf("expected confirm-fixed success")
		}
		got := mustMeta(t, metaPath)
		if got.Status != "fixed" || got.FixedBy != "bbbbbbbbbbbb" {
			t.Fatalf("meta mismatch: %#v", got)
		}
		records, err := LoadAttemptRecords(attemptPath)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 0 {
			t.Fatalf("confirm-fixed appended attempt records: %#v", records)
		}
	})

	t.Run("prior fixed attempt exits zero", func(t *testing.T) {
		cfg := testConfig(t)
		writeReplayStub(t, cfg, 0)
		writeMetaFixture(t, cfg, "bbbbbbbbbbbb", "open")
		attemptPath := filepath.Join(cfg.StateDir, "attempts", "bbbbbbbbbbbb.jsonl")
		if err := AppendAttemptRecord(attemptPath, AttemptRecord{Attempt: 1, Sig: "bbbbbbbbbbbb", Outcome: "fixed"}); err != nil {
			t.Fatal(err)
		}
		records, err := LoadAttemptRecords(attemptPath)
		if err != nil {
			t.Fatal(err)
		}
		finding := mustFinding(t, cfg, "bbbbbbbbbbbb")
		if !priorFixEvidence(finding.Meta, records) || !confirmFixed(context.Background(), cfg, finding, nil) {
			t.Fatalf("expected confirm-fixed success")
		}
		got := mustMeta(t, finding.MetaPath)
		if got.Status != "fixed" {
			t.Fatalf("status=%q, want fixed", got.Status)
		}
		after, err := LoadAttemptRecords(attemptPath)
		if err != nil {
			t.Fatal(err)
		}
		if len(after) != 1 {
			t.Fatalf("attempt record count changed: %#v", after)
		}
	})

	t.Run("replay still diverges", func(t *testing.T) {
		cfg := testConfig(t)
		writeReplayStub(t, cfg, 10)
		writeMetaFixture(t, cfg, "cccccccccccc", "open")
		finding := mustFinding(t, cfg, "cccccccccccc")
		finding.Meta.FixedBy = "aaaaaaaaaaaa"
		if confirmFixed(context.Background(), cfg, finding, nil) {
			t.Fatalf("confirmFixed succeeded for exit 10")
		}
		got := mustMeta(t, finding.MetaPath)
		if got.Status != "open" {
			t.Fatalf("status=%q, want open", got.Status)
		}
	})

	t.Run("no evidence skips replay", func(t *testing.T) {
		cfg := testConfig(t)
		marker := filepath.Join(cfg.StateDir, "replay-called")
		writeReplayStubWithMarker(t, cfg, 0, marker)
		writeMetaFixture(t, cfg, "dddddddddddd", "open")
		finding := mustFinding(t, cfg, "dddddddddddd")
		if priorFixEvidence(finding.Meta, nil) && confirmFixed(context.Background(), cfg, finding, nil) {
			t.Fatalf("unexpected confirm-fixed")
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("replay stub was invoked: %v", err)
		}
	})
}

func TestUntrackedDeletionPolicy(t *testing.T) {
	before := pathSet{"tools/ddlfuzz/existing.txt": {}}
	after := pathSet{
		"tools/ddlfuzz/existing.txt":          {},
		"flow/connectors/mysql/tmp_test.go":   {},
		"tools/ddlfuzz/scratch.txt":           {},
		"tools/ddlfuzz/state/findings/x":      {},
		"tools/ddlfuzz/build/ddlfuzz.new":     {},
		"docs/outside.md":                     {},
		"../escape":                           {},
		"/tmp/absolute":                       {},
		".git/hooks/post-commit":              {},
		"tools/ddlfuzz/state/../sneaky.txt":   {},
		"tools/ddlfuzz/oracle/mysql/tmp.cc":   {},
		"tools/ddlfuzz/build/../supervisor/x": {},
	}
	plan := PlanUntrackedDeletion(before, after, []string{"flow/", "tools/ddlfuzz/"}, []string{"tools/ddlfuzz/state/", "tools/ddlfuzz/build/"})
	gotDelete := strings.Join(plan.ToDelete, ",")
	for _, want := range []string{"flow/connectors/mysql/tmp_test.go", "tools/ddlfuzz/oracle/mysql/tmp.cc", "tools/ddlfuzz/scratch.txt"} {
		if !strings.Contains(gotDelete, want) {
			t.Fatalf("expected %s in deletion plan; got %v", want, plan.ToDelete)
		}
	}
	gotKept := strings.Join(plan.Kept, ",")
	for _, want := range []string{"tools/ddlfuzz/state/findings/x", "tools/ddlfuzz/build/ddlfuzz.new", "docs/outside.md", "../escape", "/tmp/absolute", ".git/hooks/post-commit", "tools/ddlfuzz/state/../sneaky.txt", "tools/ddlfuzz/build/../supervisor/x"} {
		if !strings.Contains(gotKept, want) {
			t.Fatalf("expected %s to be kept; got %v", want, plan.Kept)
		}
	}
}

func TestAttemptRecordRoundTrip(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.StateDir, "attempts", "deadbeef0123.jsonl")
	now := time.Date(2026, 7, 3, 1, 2, 3, 4, time.UTC)
	rec := AttemptRecord{
		Attempt:     1,
		Sig:         "deadbeef0123",
		StartedAt:   now,
		EndedAt:     now.Add(time.Minute),
		Outcome:     "fixed",
		Detail:      "ok",
		Tokens:      TokenUsage{Input: 10, CachedInput: 4, Output: 7},
		Commits:     []string{"abc"},
		ThreadID:    "thread-1",
		Transcript:  "attempts/deadbeef0123.attempt1.stream.jsonl",
		LastMessage: "attempts/deadbeef0123.attempt1.last.txt",
		Diff:        "attempts/deadbeef0123.attempt1.diff",
	}
	if err := AppendAttemptRecord(path, rec); err != nil {
		t.Fatal(err)
	}
	got, err := LoadAttemptRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Sig != rec.Sig || got[0].Tokens.Output != 7 || !got[0].StartedAt.Equal(now) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestFlapDetector(t *testing.T) {
	groups := map[string]*GroupRecord{
		"group1": {Sigs: []string{"aaa", "bbb"}, FixCount: 2, LastFixTS: "2026-07-03T00:00:00Z"},
	}
	findings := []GroupedFinding{
		{Sig: "aaa", GroupKey: "group1", Status: "fixed"},
		{Sig: "bbb", GroupKey: "group1", Status: "fixed"},
		{Sig: "ccc", GroupKey: "group1", Status: "open", RediscoveredCount: 1},
	}
	toPark := ApplyFlapDetector(groups, findings)
	if !groups["group1"].Parked {
		t.Fatalf("group was not marked parked")
	}
	if len(toPark) != 1 || toPark[0] != "ccc" {
		t.Fatalf("park list mismatch: %v", toPark)
	}
}

func TestReportRenderingFromFixtures(t *testing.T) {
	cfg := testConfig(t)
	mustWrite(t, filepath.Join(cfg.StateDir, "stats.json"), `{"ts":"2026-07-03T12:00:00Z","execs_total":123,"execs_per_sec":41,"corpus_count":{"mysql":2,"mariadb":3},"edges":{"go":10,"mysql":20,"mariadb":30},"oracle_restarts":{"mysql":1,"mariadb":0},"findings_emitted_total":4}`)
	mustWrite(t, filepath.Join(cfg.StateDir, "e2e-stats.json"), `{"updated_at":"2026-07-03T12:00:00Z","mysql":{"cases":5},"mariadb":{"cases":7},"confirmed_ok":2}`)
	mustWrite(t, filepath.Join(cfg.StateDir, "coverage", "history", "edges.csv"), "ts,go,mysql,mariadb\n2026-07-03T11:00:00Z,8,19,25\n")
	writeMetaFixture(t, cfg, "aaaaaaaaaaaa", "open")
	writeMetaFixture(t, cfg, "bbbbbbbbbbbb", "fixed")
	writeMetaFixture(t, cfg, "cccccccccccc", "ledgered")
	writeMetaFixture(t, cfg, "dddddddddddd", "parked")
	if err := AddSpend(cfg, TokenUsage{Input: 10, CachedInput: 3, Output: 4}, time.Hour); err != nil {
		t.Fatal(err)
	}
	report, err := RenderReport(cfg, "running", ComponentSnapshot{FuzzerUp: true, FuzzerRestarts: 1, E2EUp: true, E2ERestarts: 2, DiskFreeBytes: 50 * GiB})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# ddlfuzz run report", "execs_total 123", "open 1 / fixed 1 / ledgered 1 / parked 1", "input 10 cached 3 output 4", "fuzzer up true/restarts 1"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestCodexJSONLParsingFromFixture(t *testing.T) {
	dir := t.TempDir()
	stream := filepath.Join(dir, "stream.jsonl")
	last := filepath.Join(dir, "last.txt")
	mustWrite(t, stream, strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-xyz"}`,
		`{"type":"item.completed","item":{"type":"agent_message","text":"I changed the parser."}}`,
		`{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":40,"output_tokens":30,"reasoning_output_tokens":12}}`,
		`{"type":"turn.completed","usage":{"input_tokens":5,"cached_input_tokens":1,"output_tokens":2}}`,
	}, "\n")+"\n")
	mustWrite(t, last, "done\nRESULT: fixed deadbeef0123\n")
	parsed, err := ParseCodexJSONL(stream, last)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ThreadID != "thread-xyz" || parsed.Tokens.Input != 105 || parsed.Tokens.CachedInput != 41 || parsed.Tokens.Output != 32 {
		t.Fatalf("parsed usage mismatch: %#v", parsed)
	}
	if parsed.ResultOutcome != "fixed" || parsed.ResultSig != "deadbeef0123" {
		t.Fatalf("result parse mismatch: %#v", parsed)
	}
	if len(parsed.AgentMessages) != 1 || !strings.Contains(parsed.AgentMessages[0], "parser") {
		t.Fatalf("agent message missing: %#v", parsed.AgentMessages)
	}
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeMetaFixture(t *testing.T, cfg Config, sig, status string) {
	t.Helper()
	meta := fmt.Sprintf(`{"sig":%q,"engine":"mysql","sql_mode":0,"lane":"fast","our_sig":"alter t{}","oracle_digest":{"signature":"alter t{col c=int}"},"status":%q,"discovered_at":"2026-07-03T12:00:00Z"}`, sig, status)
	mustWrite(t, filepath.Join(cfg.StateDir, "findings", sig, "meta.json"), meta)
}

func writeReplayStub(t *testing.T, cfg Config, exitCode int) {
	t.Helper()
	writeReplayStubWithMarker(t, cfg, exitCode, "")
}

func writeReplayStubWithMarker(t *testing.T, cfg Config, exitCode int, marker string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(cfg.DDLfuzzBin), 0o755); err != nil {
		t.Fatal(err)
	}
	var script string
	if marker != "" {
		script = fmt.Sprintf("#!/bin/sh\nprintf called > %q\nexit %d\n", marker, exitCode)
	} else {
		script = fmt.Sprintf("#!/bin/sh\nexit %d\n", exitCode)
	}
	if err := os.WriteFile(cfg.DDLfuzzBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustFinding(t *testing.T, cfg Config, sig string) Finding {
	t.Helper()
	findings, err := ScanFindings(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		if f.Sig == sig {
			return f
		}
	}
	t.Fatalf("finding %s not found", sig)
	return Finding{}
}

func mustMeta(t *testing.T, path string) FindingMeta {
	t.Helper()
	meta, err := loadFindingMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}
