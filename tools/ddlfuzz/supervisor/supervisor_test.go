package main

import (
	"context"
	"encoding/json"
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
	stmtA := GroupInfoForMeta(FindingMeta{Class: "sig_mismatch", Shape: "stmt_count(alter+rename≠alter)"})
	stmtB := GroupInfoForMeta(FindingMeta{Class: "sig_mismatch", Shape: "stmt_count(alter+alter≠alter)"})
	if stmtA.Shape != "stmt_count" || stmtB.Shape != "stmt_count" || stmtA.Key != stmtB.Key {
		t.Fatalf("stmt_count dimension prefix mismatch: %#v %#v", stmtA, stmtB)
	}
	colAttr := GroupInfoForMeta(FindingMeta{Class: "e2e-col-attr", Shape: "col-attr(nullability)"})
	if colAttr.Shape != "col-attr" {
		t.Fatalf("col-attr dimension prefix=%q", colAttr.Shape)
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

func TestTouchedParserOrOracle(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		want  bool
	}{
		{name: "parser", paths: []string{"flow/connectors/mysql/ddl_parser.go"}, want: true},
		{name: "parser shim", paths: []string{"flow/connectors/mysql/ddlfuzz_export.go"}, want: true},
		{name: "mysql oracle", paths: []string{"tools/ddlfuzz/oracle/mysql/driver.cc"}, want: true},
		{name: "mariadb oracle", paths: []string{"tools/ddlfuzz/oracle/mariadb/digest.cc"}, want: true},
		{name: "compare only", paths: []string{"tools/ddlfuzz/internal/compare/compare.go"}},
		{name: "supervisor", paths: []string{"tools/ddlfuzz/supervisor/gate.go"}},
		{name: "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := touchedParserOrOracle(tt.paths); got != tt.want {
				t.Fatalf("touchedParserOrOracle(%v)=%v, want %v", tt.paths, got, tt.want)
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
	t.Run("reproducing new sig freezes group", func(t *testing.T) {
		groups := map[string]*GroupRecord{
			"group1": {Sigs: []string{"aaa", "bbb"}, FixCount: 2, LastFixTS: "2026-07-03T00:00:00Z"},
		}
		frozen := ApplyFlapDetector(groups, []GroupedFinding{
			{Sig: "aaa", GroupKey: "group1", Status: "fixed"},
			{Sig: "bbb", GroupKey: "group1", Status: "fixed"},
			{Sig: "ccc", GroupKey: "group1", Status: "open", Reproduces: true},
		})
		if !groups["group1"].Parked {
			t.Fatalf("group was not marked parked")
		}
		if len(frozen) != 1 || frozen[0] != "group1" {
			t.Fatalf("frozen groups mismatch: %v", frozen)
		}
	})
	t.Run("reopen not reproducing does not freeze", func(t *testing.T) {
		groups := map[string]*GroupRecord{"group1": {Sigs: []string{"aaa"}, FixCount: 2}}
		frozen := ApplyFlapDetector(groups, []GroupedFinding{
			{Sig: "aaa", GroupKey: "group1", Status: "open", ReopenedCount: 1, Reproduces: false},
		})
		if groups["group1"].Parked || len(frozen) != 0 {
			t.Fatalf("non-reproducing reopen froze group: parked=%v frozen=%v", groups["group1"].Parked, frozen)
		}
	})
	t.Run("reproducing reopen freezes", func(t *testing.T) {
		groups := map[string]*GroupRecord{"group1": {Sigs: []string{"aaa"}, FixCount: 2}}
		frozen := ApplyFlapDetector(groups, []GroupedFinding{
			{Sig: "aaa", GroupKey: "group1", Status: "open", ReopenedCount: 1, Reproduces: true},
		})
		if !groups["group1"].Parked || len(frozen) != 1 {
			t.Fatalf("reproducing reopen did not freeze: parked=%v frozen=%v", groups["group1"].Parked, frozen)
		}
	})
}

func TestWriteFindingMetaFieldsRoundTrip(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.StateDir, "findings", "aaaaaaaaaaaa", "meta.json")
	mustWrite(t, path, `{"sig":"aaaaaaaaaaaa","status":"open","times_seen":7,"submitted_text":"ALTER TABLE secret ADD c INT","descriptor":{"v":1}}`)
	if err := writeFindingMetaFields(path, map[string]any{"status": "fixed", "fixed_by": "bbbbbbbbbbbb"}); err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(mustRead(t, path), &meta); err != nil {
		t.Fatal(err)
	}
	if meta["status"] != "fixed" || meta["fixed_by"] != "bbbbbbbbbbbb" || int(meta["times_seen"].(float64)) != 7 || meta["submitted_text"] == nil || meta["descriptor"] == nil {
		t.Fatalf("raw meta fields not preserved: %#v", meta)
	}
}

func TestApplyFlapScanSoftFreeze(t *testing.T) {
	cfg := testConfig(t)
	writeReplayStub(t, cfg, 10)
	sig := "cccccccccccc"
	writeMetaFixture(t, cfg, sig, "open")
	finding := mustFinding(t, cfg, sig)
	finding.Group = GroupInfo{Key: "group1", Class: "sig_mismatch", Shape: "stmt_count"}
	groups := map[string]*GroupRecord{"group1": {Sigs: []string{"aaaaaaaaaaaa", "bbbbbbbbbbbb"}, FixCount: 2}}

	if err := ApplyFlapScanAndPark(cfg, groups, []Finding{finding}); err != nil {
		t.Fatal(err)
	}
	if !groups["group1"].Parked {
		t.Fatalf("group was not frozen")
	}
	parked, err := LoadParkedList(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(parked) != 0 {
		t.Fatalf("soft freeze wrote hard parked list: %v", parked)
	}
	if got := mustMeta(t, finding.MetaPath); got.Status != "open" {
		t.Fatalf("soft freeze changed member status to %q", got.Status)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, "escalations", "run-flap-group1.md")); err != nil {
		t.Fatalf("run-level escalation missing: %v", err)
	}
	if _, _, ok := SelectFinding(cfg, []Finding{finding}, parked, groups); ok {
		t.Fatalf("SelectFinding selected from frozen group")
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

func TestRatePositiveDeltaSum(t *testing.T) {
	base := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	samples := []SampleRecord{{TS: base, Fuzz: SampleFuzz{ExecsTotal: 0}}, {TS: base.Add(30 * time.Second), Fuzz: SampleFuzz{ExecsTotal: 300}}, {TS: base.Add(time.Minute), Fuzz: SampleFuzz{ExecsTotal: 600}}}
	rate, covered, ok := Rate(samples, func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, time.Minute, base.Add(time.Minute))
	if !ok || rate != 10 || covered != time.Minute {
		t.Fatalf("steady rate=%v covered=%v ok=%v", rate, covered, ok)
	}
	reset := []SampleRecord{{TS: base, Fuzz: SampleFuzz{ExecsTotal: 1000}}, {TS: base.Add(30 * time.Second), Fuzz: SampleFuzz{ExecsTotal: 0}}, {TS: base.Add(time.Minute), Fuzz: SampleFuzz{ExecsTotal: 400}}}
	rate, _, ok = Rate(reset, func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, time.Minute, base.Add(time.Minute))
	if !ok || rate != float64(400)/60 {
		t.Fatalf("reset rate=%v ok=%v", rate, ok)
	}
	_, _, ok = Rate(samples[:1], func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, time.Minute, base)
	if ok {
		t.Fatalf("single sample should not produce a rate")
	}
	rate, covered, ok = Rate(samples[1:], func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, 15*time.Minute, base.Add(time.Minute))
	if !ok || covered != 30*time.Second || rate != 10 {
		t.Fatalf("short rate=%v covered=%v ok=%v", rate, covered, ok)
	}
}

func TestReadSamplesTail(t *testing.T) {
	cfg := testConfig(t)
	base := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	var lines []string
	for i := 0; i < 20; i++ {
		data, err := json.Marshal(SampleRecord{TS: base.Add(time.Duration(i) * time.Second), Fuzz: SampleFuzz{ExecsTotal: int64(i)}})
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(data))
	}
	mustWrite(t, filepath.Join(cfg.StateDir, "samples.jsonl"), strings.Join(lines, "\n")+"\n")
	got := ReadSamplesTail(cfg, 1200)
	if len(got) == 0 || got[0].Fuzz.ExecsTotal == 0 || got[len(got)-1].Fuzz.ExecsTotal != 19 {
		t.Fatalf("tail parse mismatch: %#v", got)
	}
	cfg2 := testConfig(t)
	if got := ReadSamplesTail(cfg2, 100); got != nil {
		t.Fatalf("missing samples got %#v", got)
	}
	mustWrite(t, filepath.Join(cfg2.StateDir, "samples.jsonl"), "")
	if got := ReadSamplesTail(cfg2, 100); got != nil {
		t.Fatalf("empty samples got %#v", got)
	}
}

func TestSparkline(t *testing.T) {
	base := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	samples := []SampleRecord{{TS: base, Fuzz: SampleFuzz{ExecsTotal: 0}}, {TS: base.Add(time.Minute), Fuzz: SampleFuzz{ExecsTotal: 10}}, {TS: base.Add(2 * time.Minute), Fuzz: SampleFuzz{ExecsTotal: 30}}, {TS: base.Add(4 * time.Minute), Fuzz: SampleFuzz{ExecsTotal: 70}}}
	if got := Sparkline(samples, func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, 4*time.Minute, 4); got != "·▄██" {
		t.Fatalf("sparkline=%q", got)
	}
	if got := Sparkline(samples[:1], func(s SampleRecord) int64 { return s.Fuzz.ExecsTotal }, 4*time.Minute, 3); got != "···" {
		t.Fatalf("empty sparkline=%q", got)
	}
}

func TestGroupRows(t *testing.T) {
	cfg := testConfig(t)
	now := time.Date(2026, 7, 3, 15, 0, 0, 0, time.UTC)
	writeStatusMeta(t, cfg, "sig1", "mysql", "classA", "shapeA", "2026-07-03T12:00:00Z")
	writeStatusMeta(t, cfg, "sig2", "mariadb", "classA", "shapeA", "2026-07-03T13:00:00Z")
	writeStatusMeta(t, cfg, "sig3", "mysql", "classB", "shapeB", "2026-07-03T14:00:00Z")
	writeStatusMeta(t, cfg, "sig4", "mariadb", "classC", "shapeC", "2026-07-03T14:30:00Z")
	for _, sig := range []string{"sig1", "sig2", "sig3"} {
		if err := AppendAttemptRecord(filepath.Join(cfg.StateDir, "attempts", sig+".jsonl"), AttemptRecord{Attempt: 1, Sig: sig}); err != nil {
			t.Fatal(err)
		}
	}
	groupA := GroupInfoForMeta(FindingMeta{Class: "classA", Shape: "shapeA"}).Key
	if err := SaveGroups(cfg, map[string]*GroupRecord{groupA: &GroupRecord{Parked: true, FixCount: 2}}); err != nil {
		t.Fatal(err)
	}
	rows, more := BuildGroupRows(cfg, now, 10)
	if more != 0 || len(rows) != 3 {
		t.Fatalf("rows=%#v more=%d", rows, more)
	}
	if rows[0].Sigs != 2 || rows[0].Attempts != 2 || rows[0].MySQL != 1 || rows[0].MariaDB != 1 || rows[0].OldestAge != "3h" || !strings.Contains(rows[0].Flags, "flap-parked") {
		t.Fatalf("group row mismatch: %#v", rows[0])
	}
}

func TestCurrentAttemptStale(t *testing.T) {
	cfg := testConfig(t)
	now := time.Now()
	cur := CurrentAttempt{Sig: "sig", Attempt: 1, MaxAttempts: 3, StartedAt: now.Add(-time.Hour), AttemptDeadline: now.Add(-10 * time.Minute)}
	if err := atomicWriteJSON(filepath.Join(cfg.StateDir, "current-attempt.json"), cur, 0o644); err != nil {
		t.Fatal(err)
	}
	got := collectFixAgent(cfg, now, RunStatus{Alive: false}, SpendRecord{})
	if got.State != "stale" || !got.Stale {
		t.Fatalf("stale attempt mismatch: %#v", got)
	}
	cur.AttemptDeadline = now.Add(time.Hour)
	if err := atomicWriteJSON(filepath.Join(cfg.StateDir, "current-attempt.json"), cur, 0o644); err != nil {
		t.Fatal(err)
	}
	got = collectFixAgent(cfg, now, RunStatus{Alive: true}, SpendRecord{})
	if got.State != "running" || got.Stale {
		t.Fatalf("running attempt mismatch: %#v", got)
	}
	if err := os.Remove(filepath.Join(cfg.StateDir, "current-attempt.json")); err != nil {
		t.Fatal(err)
	}
	got = collectFixAgent(cfg, now, RunStatus{Alive: true}, SpendRecord{})
	if got.State != "idle" {
		t.Fatalf("missing attempt should be idle: %#v", got)
	}
}

func TestMetricTableAlignment(t *testing.T) {
	rows := metricTable([]string{"METRIC", "NOW", "Δ1m"}, [][]string{{paint(true, sgrGreen, "execs/s"), "10", "2"}, {"very-long-metric-name", "1000", "30"}}, 80, true)
	if visibleWidth(paint(true, sgrRed, "abc")) != 3 {
		t.Fatalf("visibleWidth counts SGR")
	}
	if !strings.Contains(rows[1], "    10") || !strings.Contains(rows[2], "  1000") {
		t.Fatalf("numeric columns not right aligned: %#v", rows)
	}
	truncated := metricTable([]string{"METRIC", "NOW"}, [][]string{{"metric", "abcdefghijklmnopqrstuvwxyz"}}, 18, false)
	if visibleWidth(truncated[1]) > 18 || !strings.HasSuffix(truncated[1], "…") {
		t.Fatalf("truncate mismatch: %#v", truncated)
	}
}

func TestSideBySide(t *testing.T) {
	got := sideBySide([]string{paint(true, sgrGreen, "A"), "BBBB"}, []string{"one"}, 6, 3)
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
	if !strings.HasSuffix(got[0], "one") || visibleWidth(stripANSI(strings.TrimSuffix(got[0], "one"))) != 9 {
		t.Fatalf("first line padding/gutter mismatch: %q", got[0])
	}
	if visibleWidth(got[1]) != 9 {
		t.Fatalf("blank right side width=%d line=%q", visibleWidth(got[1]), got[1])
	}
}

func TestFrameBreakpoint(t *testing.T) {
	snap := statusFixture(t)
	wide := RenderStatus(snap, false, 130)
	if fast, e2e := strings.Index(wide, "FAST LANE"), strings.Index(wide, "E2E LANE"); fast < 0 || e2e < 0 || lineOf(wide, fast) != lineOf(wide, e2e) {
		t.Fatalf("wide lanes not paired:\n%s", wide)
	}
	narrow := RenderStatus(snap, false, 100)
	order := []string{"FAST LANE", "E2E LANE", "FINDINGS", "FIX AGENT", "EVENTS"}
	last := -1
	for _, pane := range order {
		idx := strings.Index(narrow, pane)
		if idx <= last {
			t.Fatalf("pane order mismatch for %s:\n%s", pane, narrow)
		}
		last = idx
	}
}

func TestRenderStatusSmoke(t *testing.T) {
	snap := statusFixture(t)
	out := RenderStatus(snap, false, 130)
	for _, want := range []string{"ddlfuzz", "FAST LANE", "E2E LANE", "FINDINGS", "FIX AGENT", "EVENTS", "METRIC", "classA|shapea", "n/a"} {
		if !strings.Contains(out, want) {
			t.Fatalf("render missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\x1b[") {
		t.Fatalf("color disabled but ANSI present")
	}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if visibleWidth(line) > 130 {
			t.Fatalf("line too wide (%d): %q", visibleWidth(line), line)
		}
	}
}

func TestEventsFilter(t *testing.T) {
	cfg := testConfig(t)
	log := strings.Join([]string{"2026-07-03T12:00:00Z fuzzer: execs/s=1 restarts=my:0,ma:0", "2026-07-03T12:00:01Z e2e: heartbeat cases=1", "2026-07-03T12:00:02Z fuzzer exited: boom", "2026-07-03T12:00:03Z fix committed for abc"}, "\n") + "\n"
	mustWrite(t, filepath.Join(cfg.StateDir, "supervisor.log"), log)
	events := readEvents(cfg)
	if len(events) != 2 || !strings.Contains(events[0].Message, "exited") || !strings.Contains(events[1].Message, "fix committed") {
		t.Fatalf("events=%#v", events)
	}
}

func TestStatusJSON(t *testing.T) {
	snap := statusFixture(t)
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var round StatusSnapshot
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatal(err)
	}
	if round.Findings.Counts.Open == 0 || round.Run.FixModel == "" {
		t.Fatalf("round trip mismatch: %#v", round)
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

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
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

func writeStatusMeta(t *testing.T, cfg Config, sig, engine, class, shape, discovered string) {
	t.Helper()
	meta := FindingMeta{Sig: sig, Engine: engine, Status: "open", DiscoveredAt: discovered, Class: class, Shape: shape, OurSig: "ours", OracleDigest: json.RawMessage(`{"signature":"oracle"}`)}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(cfg.StateDir, "findings", sig, "meta.json"), string(data))
}

func statusFixture(t *testing.T) StatusSnapshot {
	t.Helper()
	cfg := testConfig(t)
	now := time.Date(2026, 7, 3, 15, 0, 0, 0, time.UTC)
	cfg.StartedAt = now.Add(-3 * time.Hour)
	cfg.Deadline = now.Add(61 * time.Hour)
	cfg.RunHours = 72
	cfg.FixModel = "gpt-5.5"
	if err := WriteRunState(cfg); err != nil {
		t.Fatal(err)
	}
	writeStatusMeta(t, cfg, "sig1", "mysql", "classA", "shapeA", "2026-07-03T12:00:00Z")
	writeStatusMeta(t, cfg, "sig2", "mariadb", "classA", "shapeA", "2026-07-03T13:00:00Z")
	mustWrite(t, filepath.Join(cfg.StateDir, "stats.json"), `{"ts":"2026-07-03T14:59:56Z","execs_total":1000,"execs_per_sec":405000,"findings_emitted_total":10,"corpus_count":{"mysql":77600,"mariadb":66400},"edges":{"go":1,"mysql":20987,"mariadb":10416},"oracle_restarts":{"mysql":0,"mariadb":0}}`)
	mustWrite(t, filepath.Join(cfg.StateDir, "e2e-stats.json"), `{"updated_at":1783090797,"cases":{"mysql":10,"mariadb":12},"exec_rejects":{"mysql":3,"mariadb":4}}`)
	mustWrite(t, filepath.Join(cfg.StateDir, "supervisor.log"), "2026-07-03T14:59:58Z fix committed for sig1\n")
	samples := []SampleRecord{
		{TS: now.Add(-time.Minute), Fuzz: SampleFuzz{ExecsTotal: 100, Suppressed: 1, Edges: map[string]int64{"mysql": 10, "mariadb": 5}}, E2E: SampleE2E{Cases: map[string]int64{"mysql": 1, "mariadb": 1}, ExecRejects: map[string]int64{"mysql": 1}}},
		{TS: now, Fuzz: SampleFuzz{ExecsTotal: 700, Suppressed: 7, Edges: map[string]int64{"mysql": 13, "mariadb": 5}}, E2E: SampleE2E{Cases: map[string]int64{"mysql": 5, "mariadb": 5}, ExecRejects: map[string]int64{"mysql": 2, "mariadb": 2}}},
	}
	for _, rec := range samples {
		if err := AppendSample(cfg, rec); err != nil {
			t.Fatal(err)
		}
	}
	return CollectStatus(cfg)
}

func lineOf(s string, idx int) int {
	return strings.Count(s[:idx], "\n")
}
