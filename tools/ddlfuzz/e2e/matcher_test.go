package e2e

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// testStatusVars encodes Q_FLAGS2 (code 0, 4 bytes) followed by Q_SQL_MODE
// (code 1, 8 bytes LE) — the prefix both engines always emit.
func testStatusVars(mode uint64) []byte {
	out := append(make([]byte, 0, 14), 0x00, 0, 0, 0, 0, 0x01)
	var m [8]byte
	binary.LittleEndian.PutUint64(m[:], mode)
	return append(out, m[:]...)
}

func newTestMatcher(t *testing.T) (*matcher, *expectQueue, chan error) {
	t.Helper()
	stateDir := t.TempDir()
	if err := ensureStateLayout(stateDir); err != nil {
		t.Fatal(err)
	}
	ec := engineConfig{Name: EngineMySQL}
	errs := make(chan error, 4)
	q := newExpectQueue()
	m := &matcher{
		ec:       ec,
		stateDir: stateDir,
		queues:   map[string]*expectQueue{"fuzz_w1": q},
		stats:    NewStats(stateDir, []engineConfig{ec}),
		errs:     errs,
	}
	return m, q, errs
}

func testQueryEvent(query string) (*replication.BinlogEvent, *replication.QueryEvent) {
	qe := &replication.QueryEvent{
		Schema:     []byte("fuzz_w1"),
		Query:      []byte(query),
		StatusVars: testStatusVars(0),
	}
	return &replication.BinlogEvent{Event: qe}, qe
}

func findingClassesOnDisk(t *testing.T, stateDir string) map[string]int {
	t.Helper()
	out := map[string]int{}
	entries, err := os.ReadDir(filepath.Join(stateDir, "findings"))
	if err != nil {
		return out
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(stateDir, "findings", ent.Name(), "meta.json"))
		if err != nil {
			continue
		}
		var meta struct {
			Class string `json:"class"`
		}
		if json.Unmarshal(data, &meta) == nil && meta.Class != "" {
			out[meta.Class]++
		}
	}
	return out
}

func requireNoClasses(t *testing.T, classes map[string]int, forbidden ...string) {
	t.Helper()
	for _, class := range forbidden {
		if classes[class] != 0 {
			t.Fatalf("findings = %v, must not contain %s", classes, class)
		}
	}
}

func readQueueDone(t *testing.T, stateDir, sig string) queueResult {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(stateDir, "e2e-queue", "done", sig+".json"))
	if err != nil {
		t.Fatalf("done file for %s: %v", sig, err)
	}
	var res queueResult
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("done file for %s: %v", sig, err)
	}
	return res
}

func alignBaseSnapshot() snapshot {
	return snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"c":  {Name: "c", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
	}
}

func fixtureOnlyTables() map[string]bool {
	return map[string]bool{fixtureTable: true}
}

func testDDLExpect(caseID, stmt string, before, after snapshot) caseExpect {
	return caseExpect{
		Kind:         expectDDL,
		WorkerID:     1,
		CaseID:       caseID,
		Submitted:    stmt,
		Before:       before,
		After:        after,
		BeforeTables: fixtureOnlyTables(),
		AfterTables:  fixtureOnlyTables(),
		Source:       "gen",
	}
}

// The R2 desync scenario: a driver-errored-but-server-applied statement (ghost
// event) between two markers must pair with its own uncertain expectation
// instead of shifting every later pairing by one.
func TestProcessEventGhostBetweenMarkersDoesNotShiftPairing(t *testing.T) {
	m, q, errs := newTestMatcher(t)
	ctx := t.Context()
	before := alignBaseSnapshot()
	after := withRows(before, colRow{Name: "n1", Ordinal: 3, ColumnType: "int", IsNullable: "YES"})

	marker1 := markerSQL("n_1_1")
	q.Push(caseExpect{Kind: expectMarker, WorkerID: 1, CaseID: "n_1_1", Submitted: marker1})
	ev, qe := testQueryEvent(marker1)
	if err := m.processEvent(ctx, q, ev, qe); err != nil {
		t.Fatal(err)
	}

	ghost := "ALTER TABLE `fixture` ADD COLUMN `g1` int"
	q.Push(caseExpect{
		Kind:         expectUncertain,
		WorkerID:     1,
		CaseID:       "n_1_1",
		Submitted:    ghost,
		Before:       before,
		After:        withRows(before, colRow{Name: "g1", Ordinal: 3, ColumnType: "int", IsNullable: "YES"}),
		BeforeTables: fixtureOnlyTables(),
		AfterTables:  fixtureOnlyTables(),
		DriverError:  "driver: bad connection",
	})
	marker2 := markerSQL("n_1_2")
	q.Push(caseExpect{Kind: expectMarker, WorkerID: 1, CaseID: "n_1_2", Submitted: marker2})
	ddl2 := "ALTER TABLE `fixture` ADD COLUMN `n1` int"
	q.Push(testDDLExpect("n_1_2", ddl2, before, after))

	for _, query := range []string{ghost, marker2, ddl2} {
		ev, qe := testQueryEvent(query)
		if err := m.processEvent(ctx, q, ev, qe); err != nil {
			t.Fatal(err)
		}
	}

	if q.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", q.Len())
	}
	snap := m.stats.Snapshot()[EngineMySQL]
	if snap.Markers != 2 || snap.MatchedDDLs != 1 || snap.ExecRejectApplied != 1 {
		t.Fatalf("stats = %+v, want markers=2 matched_ddls=1 exec_reject_applied=1", snap)
	}
	classes := findingClassesOnDisk(t, m.stateDir)
	if classes["e2e-exec-reject-applied"] != 1 {
		t.Fatalf("findings = %v, want one e2e-exec-reject-applied", classes)
	}
	requireNoClasses(t, classes,
		"e2e-query-rewrite", "e2e-plumbing-sig", "e2e-unexpected-event",
		"e2e-missing-event", "e2e-statusvar-walk", "e2e-missed-column-effect")
	select {
	case err := <-errs:
		t.Fatalf("unexpected harness error: %v", err)
	default:
	}
}

func TestProcessEventUnknownExtraEventIsLoudAndConsumesNothing(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	ctx := t.Context()
	marker2 := markerSQL("n_1_2")
	q.Push(caseExpect{Kind: expectMarker, CaseID: "n_1_2", Submitted: marker2})
	q.Push(testDDLExpect("n_1_2", "ALTER TABLE `fixture` ADD COLUMN `n1` int", alignBaseSnapshot(), alignBaseSnapshot()))

	ev, qe := testQueryEvent("OPTIMIZE TABLE `t1`")
	if err := m.processEvent(ctx, q, ev, qe); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 2 {
		t.Fatalf("queue length = %d, want 2 (nothing consumed)", q.Len())
	}
	if got := m.stats.Snapshot()[EngineMySQL].UnexpectedEvents; got != 1 {
		t.Fatalf("unexpected_events = %d, want 1", got)
	}
	if classes := findingClassesOnDisk(t, m.stateDir); classes["e2e-unexpected-event"] != 1 {
		t.Fatalf("findings = %v, want one e2e-unexpected-event", classes)
	}

	ev, qe = testQueryEvent(marker2)
	if err := m.processEvent(ctx, q, ev, qe); err != nil {
		t.Fatal(err)
	}
	if got := m.stats.Snapshot()[EngineMySQL].Markers; got != 1 {
		t.Fatalf("markers = %d, want 1 (marker re-anchored)", got)
	}
	if q.Len() != 1 {
		t.Fatalf("queue length = %d, want 1", q.Len())
	}
}

func TestProcessEventMissingMarkerEventHealsWithVerbatimDDL(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	before := alignBaseSnapshot()
	after := withRows(before, colRow{Name: "n1", Ordinal: 3, ColumnType: "int", IsNullable: "YES"})
	q.Push(caseExpect{Kind: expectMarker, CaseID: "n_1_3", Submitted: markerSQL("n_1_3")})
	ddl := "ALTER TABLE `fixture` ADD COLUMN `n1` int"
	q.Push(testDDLExpect("n_1_3", ddl, before, after))

	ev, qe := testQueryEvent(ddl)
	if err := m.processEvent(t.Context(), q, ev, qe); err != nil {
		t.Fatal(err)
	}
	snap := m.stats.Snapshot()[EngineMySQL]
	if snap.SkippedControls != 1 || snap.MatchedDDLs != 1 || snap.Markers != 0 {
		t.Fatalf("stats = %+v, want skipped_controls=1 matched_ddls=1 markers=0", snap)
	}
	if classes := findingClassesOnDisk(t, m.stateDir); len(classes) != 0 {
		t.Fatalf("findings = %v, want none", classes)
	}
	if q.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", q.Len())
	}
}

func TestMarkerAnchorReportsMissingDDLEvent(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	before := alignBaseSnapshot()
	after := withRows(before, colRow{Name: "n1", Ordinal: 3, ColumnType: "int", IsNullable: "YES"})
	exp := testDDLExpect("n_1_4", "ALTER TABLE `fixture` ADD COLUMN `n1` int", before, after)
	exp.Queue = &queueItem{Sig: "sig-a", Engine: EngineMySQL, Statement: exp.Submitted}
	q.Push(exp)
	q.Push(caseExpect{Kind: expectControl, Submitted: controlDropSQL(fixtureTable)})
	marker := markerSQL("n_1_5")
	q.Push(caseExpect{Kind: expectMarker, CaseID: "n_1_5", Submitted: marker})

	ev, qe := testQueryEvent(marker)
	if err := m.processEvent(t.Context(), q, ev, qe); err != nil {
		t.Fatal(err)
	}
	snap := m.stats.Snapshot()[EngineMySQL]
	if snap.MissingEvents != 1 || snap.SkippedControls != 1 || snap.Markers != 1 {
		t.Fatalf("stats = %+v, want missing_events=1 skipped_controls=1 markers=1", snap)
	}
	if classes := findingClassesOnDisk(t, m.stateDir); classes["e2e-missing-event"] != 1 {
		t.Fatalf("findings = %v, want one e2e-missing-event", classes)
	}
	if res := readQueueDone(t, m.stateDir, "sig-a"); res.Result != "still-diverges" {
		t.Fatalf("queue result = %+v, want still-diverges", res)
	}
	if q.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", q.Len())
	}
}

// A statement the server treats as comment-only succeeds with no binlog event
// and no schema change; skipping its expectation must stay quiet.
func TestNoopDDLSkipBeforeControlIsBenign(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	base := alignBaseSnapshot()
	exp := testDDLExpect("n_1_6",
		"/*!100000 SET STATEMENT max_statement_time=60 FOR ALTER TABLE fixture DROP COLUMN id */",
		base, base)
	exp.Queue = &queueItem{Sig: "sig-n", Engine: EngineMySQL, Statement: exp.Submitted}
	q.Push(exp)
	q.Push(caseExpect{Kind: expectControl, Submitted: "DROP TABLE IF EXISTS `fixture`"})

	ev, qe := testQueryEvent("DROP TABLE IF EXISTS `fixture` /* generated by server */")
	if err := m.processEvent(t.Context(), q, ev, qe); err != nil {
		t.Fatal(err)
	}
	snap := m.stats.Snapshot()[EngineMySQL]
	if snap.NoopDDLs != 1 || snap.Controls != 1 {
		t.Fatalf("stats = %+v, want noop_ddls=1 controls=1", snap)
	}
	if classes := findingClassesOnDisk(t, m.stateDir); len(classes) != 0 {
		t.Fatalf("findings = %v, want none", classes)
	}
	if res := readQueueDone(t, m.stateDir, "sig-n"); res.Result != "exec-reject" {
		t.Fatalf("queue result = %+v, want exec-reject", res)
	}
	if q.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", q.Len())
	}
}

func TestControlChainSkipsMissingControlEvent(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	q.Push(caseExpect{Kind: expectControl, Submitted: controlDropSQL("fixture_r_9")})
	create := fixtureCreateSQL(false)
	q.Push(caseExpect{Kind: expectControl, Submitted: create})

	ev, qe := testQueryEvent(create)
	if err := m.processEvent(t.Context(), q, ev, qe); err != nil {
		t.Fatal(err)
	}
	snap := m.stats.Snapshot()[EngineMySQL]
	if snap.SkippedControls != 1 || snap.Controls != 1 {
		t.Fatalf("stats = %+v, want skipped_controls=1 controls=1", snap)
	}
	if classes := findingClassesOnDisk(t, m.stateDir); len(classes) != 0 {
		t.Fatalf("findings = %v, want none", classes)
	}
}

// A control-shaped DDL under test still pairs with its own verbatim event.
func TestDecideAlignControlShapedDDLPairsVerbatim(t *testing.T) {
	items := []caseExpect{{Kind: expectDDL, Submitted: "DROP TABLE IF EXISTS `fixture`"}}
	d := decideAlign("DROP TABLE IF EXISTS `fixture`", items, false)
	if d.kind != alignDecisionMatch || d.index != 0 {
		t.Fatalf("decision = %+v, want match at 0", d)
	}
}

// Steady-state race: the DDL event can reach the matcher before the worker
// finishes the after-snapshot and pushes the expectation.
func TestAlignEventWaitsForLatePush(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	ddl := "ALTER TABLE `fixture` ADD COLUMN `n1` int"
	go func() {
		time.Sleep(30 * time.Millisecond)
		q.Push(testDDLExpect("n_1_7", ddl, alignBaseSnapshot(), alignBaseSnapshot()))
	}()
	_, qe := testQueryEvent(ddl)
	out, err := m.alignEvent(t.Context(), q, qe)
	if err != nil || !out.matched || out.exp.CaseID != "n_1_7" {
		t.Fatalf("alignEvent = %+v, %v, want match with case n_1_7", out, err)
	}
}

func TestAlignEventContextCancelUnblocks(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	_, qe := testQueryEvent("ALTER TABLE `fixture` ADD COLUMN `n1` int")
	if _, err := m.alignEvent(ctx, q, qe); err == nil {
		t.Fatal("expected context error")
	}
}

func TestAlignEventMarkerWithoutExpectationIsHarnessError(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	q.Push(caseExpect{Kind: expectMarker, CaseID: "n_1_8", Submitted: markerSQL("n_1_8")})
	_, qe := testQueryEvent(markerSQL("n_9_9"))
	if _, err := m.alignEvent(t.Context(), q, qe); err == nil {
		t.Fatal("expected harness error for unmatched marker event")
	}
	if q.Len() != 1 {
		t.Fatalf("queue length = %d, want 1 (nothing consumed)", q.Len())
	}
}

// Rewrite detection stays intact: a DDL expectation at the head pairs with a
// textually different event and checkLiveDDL files e2e-query-rewrite.
func TestProcessEventRewrittenDDLStillPairsAndFilesRewrite(t *testing.T) {
	m, q, _ := newTestMatcher(t)
	before := alignBaseSnapshot()
	after := withRows(before, colRow{Name: "n1", Ordinal: 3, ColumnType: "int", IsNullable: "YES"})
	q.Push(testDDLExpect("n_1_9", "ALTER TABLE `fixture` ADD COLUMN `n1` int", before, after))

	ev, qe := testQueryEvent("ALTER TABLE `fixture`  ADD COLUMN `n1` int")
	if err := m.processEvent(t.Context(), q, ev, qe); err != nil {
		t.Fatal(err)
	}
	if got := m.stats.Snapshot()[EngineMySQL].MatchedDDLs; got != 1 {
		t.Fatalf("matched_ddls = %d, want 1", got)
	}
	classes := findingClassesOnDisk(t, m.stateDir)
	if classes["e2e-query-rewrite"] != 1 {
		t.Fatalf("findings = %v, want one e2e-query-rewrite", classes)
	}
	requireNoClasses(t, classes, "e2e-unexpected-event", "e2e-plumbing-sig", "e2e-missing-event")
}
