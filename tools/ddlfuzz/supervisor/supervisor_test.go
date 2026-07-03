package main

import (
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
