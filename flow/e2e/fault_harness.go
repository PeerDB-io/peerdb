package e2e

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	// faultLiveness bounds every "eventually" check. It covers two normalize
	// retry backoffs (1 and 2 minutes) plus a SyncFlow restart.
	faultLiveness = 5 * time.Minute
	// faultAttribution is how long an insert waits to become its own batch.
	faultAttribution = 30 * time.Second
	faultSample      = 500 * time.Millisecond
	faultQuiet       = 15 * time.Second
	// FaultBuffer is the normalize buffer with PEERDB_NORMALIZE_BUFFER_HOURS=0.
	FaultBuffer = 2

	FaultExtraColumn = "extra"
	// FaultLargeRowBytes is the padding of InsertLarge rows. It is above the
	// 4096-byte PEERDB_S3_BYTES_PER_AVRO_FILE of workloads that use InsertLarge,
	// so a batch of such rows is staged in several files.
	FaultLargeRowBytes = 5000
	// ProgressMirror is the progress key of a CDC mirror, which tracks one
	// sync_batch_id and normalize_batch_id for all its tables. Query CDC tracks
	// them per table, keyed by source table identifier.
	ProgressMirror = "mirror"

	unattributed = math.MaxInt64
)

// FaultRow is one source row. Extra is empty until the extra column exists.
type FaultRow struct {
	Val   string
	Extra string
	ID    int64
}

// FaultTable names one table in every system the harness reads.
type FaultTable struct {
	// Source and Dest are the names FaultSource and FaultDestination take.
	Source string
	Dest   string
	// CatalogName is the destination identifier in the catalog schema mapping.
	CatalogName string
	// ProgressKey says where the table's batch IDs are tracked: ProgressMirror
	// for CDC, or the source table identifier for Query CDC.
	ProgressKey string
	// HookTable is what testhooks.WithTable sees in per-table loops; empty for CDC.
	HookTable string
}

type FaultSource interface {
	CreateTable(ctx context.Context, table string) error
	Insert(ctx context.Context, table string, row FaultRow, withExtra bool) error
	AddExtraColumn(ctx context.Context, table string) error
	// Durable returns source-side progress that must never decrease, such as the slot LSN.
	Durable(ctx context.Context, flowName string) (map[string]int64, error)
}

// FaultBulkSource is a FaultSource that can insert several rows at once.
// InsertMany must make the rows visible to CDC atomically, so they land in one
// batch: the harness attributes them all to the first new synced batch.
type FaultBulkSource interface {
	InsertMany(ctx context.Context, table string, rows []FaultRow, withExtra bool) error
}

type FaultDestination interface {
	Rows(ctx context.Context, table string) (map[int64]FaultRow, error)
	HasExtraColumn(ctx context.Context, table string) (bool, error)
	// RunningNormalizes counts normalize statements executing against table right now.
	RunningNormalizes(ctx context.Context, table string) (int, error)
	// Throttle makes normalize writes into table slow or blocked until release.
	Throttle(ctx context.Context, table string, slowness time.Duration) (release func(), err error)
	// StaleWrites lists attempted progress decreases recorded at the destination, if any.
	StaleWrites(ctx context.Context, flowName string) ([]string, error)
	Close()
}

type BatchProgress struct {
	Synced     int64
	Normalized int64
}

type FaultProgress struct {
	// Durable holds every durable progress value that must never decrease, by name.
	Durable map[string]int64
	Batches map[string]BatchProgress
	// TableWatermarks holds per-table normalized batch IDs by destination table, for CDC.
	TableWatermarks map[string]int64
	// MissingStages lists published, unnormalized batches without a stage row, by progress key.
	MissingStages map[string][]int64
}

type FaultProgressReader func(ctx context.Context, flowName string, tables []FaultTable) (FaultProgress, error)

// FaultWorkload adapts one source/destination pair to the harness.
type FaultWorkload struct {
	Suite    Suite
	Source   FaultSource
	Dest     FaultDestination
	Progress FaultProgressReader
	// Tables names the two tables a harness run uses, given a short run ID.
	Tables func(id string) []FaultTable
	// FlowConfig builds the mirror config for those tables.
	FlowConfig func(flowName string, tables []FaultTable) *protos.FlowConnectionConfigs
	// BufferLimit is the largest synced-normalized gap any sample may show.
	BufferLimit int64
	Kind        testhooks.Workload
	// SchemaChange reports whether the source supports adding a column.
	SchemaChange bool
	// IndependentTables reports whether tables progress independently (query CDC).
	IndependentTables bool
}

type expectedRow struct {
	row      FaultRow
	table    int
	batch    int64
	hasExtra bool
}

type violation struct {
	first time.Time
	msg   string
	count int
}

// FaultHarness runs one mirror under injected faults and checks the
// sync/normalize properties against what the test can observe.
type FaultHarness struct {
	t           *testing.T
	catalog     *pgxpool.Pool
	monitorDone chan struct{}
	stopMonitor func()
	violations  map[string]*violation
	durableMax  map[string]int64
	// Attribution is trusted only while nothing has disturbed the mirror since
	// it was last fully caught up; see attributionTrusted.
	unattributedSinceCaughtUp map[string]bool
	env                       WorkflowRun
	flowName                  string
	tables                    []FaultTable
	expected                  []expectedRow
	extraAdded                []bool
	schemaBatch               []int64
	armed                     []testhooks.Arming
	releases                  []func()
	w                         FaultWorkload
	nextID                    int64
	caughtUpFails             int64
	// mu guards expected, extraAdded, schemaBatch, violations, durableMax, nextID
	// and the attribution state, which the monitor goroutine and Insert share.
	mu                     sync.Mutex
	restartedSinceCaughtUp bool
}

func faultHooksEnabled() bool {
	v, _ := strconv.ParseBool(os.Getenv(testhooks.EnvEnabled))
	return v
}

// NewFaultHarness creates the tables, starts the mirror, lands one row per
// table, and starts the monitor. id must be short: it becomes part of the
// flow name and the replication slot name.
func NewFaultHarness(w FaultWorkload, id string) *FaultHarness {
	t := w.Suite.T()
	t.Helper()
	if !faultHooksEnabled() {
		t.Skipf("%s is not set", testhooks.EnvEnabled)
	}
	ctx := t.Context()
	catalog, err := CatalogTestAccessPool()
	require.NoError(t, err)
	require.NoError(t, testhooks.EnsureTable(ctx, catalog))
	require.NoError(t, installCatalogStaleWriteTriggers(ctx, catalog))

	h := &FaultHarness{
		t:                         t,
		w:                         w,
		catalog:                   catalog,
		flowName:                  AddSuffix(w.Suite, "fault_"+id),
		tables:                    w.Tables(id),
		violations:                map[string]*violation{},
		durableMax:                map[string]int64{},
		unattributedSinceCaughtUp: map[string]bool{},
		nextID:                    1,
		monitorDone:               make(chan struct{}),
	}
	h.extraAdded = make([]bool, len(h.tables))
	h.schemaBatch = make([]int64, len(h.tables))
	for i := range h.schemaBatch {
		h.schemaBatch[i] = unattributed
	}
	t.Cleanup(h.cleanup)

	for _, table := range h.tables {
		require.NoError(t, w.Source.CreateTable(ctx, table.Source))
	}
	config := w.FlowConfig(h.flowName, h.tables)
	if config.Env == nil {
		config.Env = map[string]string{}
	}
	config.Env["PEERDB_NORMALIZE_BUFFER_HOURS"] = "0"

	tc := NewTemporalClient(t)
	h.env = ExecutePeerflow(t, tc, config)
	SetupCDCFlowStatusQuery(t, h.env, config)

	for i := range h.tables {
		h.Insert(i)
	}
	if !h.WaitCaughtUp(faultLiveness) {
		h.env.Cancel(ctx)
		t.Fatal("mirror did not land the initial rows; this is a setup failure, not a finding")
	}
	// Destination metadata tables exist only once the mirror has run.
	if d, ok := w.Dest.(interface{ AfterStart(context.Context) error }); ok {
		require.NoError(t, d.AfterStart(ctx))
	}

	monitorCtx, cancel := context.WithCancel(ctx)
	h.stopMonitor = cancel
	go h.monitor(monitorCtx)
	return h
}

func (h *FaultHarness) FlowName() string { return h.flowName }

func (h *FaultHarness) Table(i int) FaultTable { return h.tables[i] }

// Violation records a property violation. The run keeps going so every check reports.
func (h *FaultHarness) Violation(key string, format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if v, ok := h.violations[key]; ok {
		v.count++
		return
	}
	msg := fmt.Sprintf(format, args...)
	h.violations[key] = &violation{first: time.Now(), msg: msg, count: 1}
	h.t.Logf("VIOLATION %s: %s", key, msg)
}

// Arm arms a site for this harness's flow.
func (h *FaultHarness) Arm(site testhooks.Site, after *testhooks.Site, hookTable string) {
	h.t.Helper()
	a := testhooks.Arming{Flow: h.flowName, Site: site, After: after, Table: hookTable}
	require.NoError(h.t, testhooks.Arm(h.t.Context(), h.catalog, a))
	h.armed = append(h.armed, a)
}

// WaitFired waits for a site to fire. A site that never fires is reported as
// not reached, not as a violation.
func (h *FaultHarness) WaitFired(site testhooks.Site, timeout time.Duration) bool {
	h.t.Helper()
	return h.pollUntil(timeout, func() bool {
		fired, _, err := testhooks.Fired(h.t.Context(), h.catalog, h.flowName, site)
		if err != nil {
			h.t.Log(err)
		}
		return fired
	})
}

func (h *FaultHarness) Release(site testhooks.Site) {
	h.t.Helper()
	require.NoError(h.t, testhooks.Release(h.t.Context(), h.catalog, h.flowName, site))
}

// Throttle slows normalize writes into table i until the returned release runs.
func (h *FaultHarness) Throttle(i int, slowness time.Duration) func() {
	h.t.Helper()
	release, err := h.w.Dest.Throttle(h.t.Context(), h.tables[i].Dest, slowness)
	require.NoError(h.t, err)
	var once sync.Once
	wrapped := func() { once.Do(release) }
	h.releases = append(h.releases, wrapped)
	return wrapped
}

// AddExtraColumn adds the extra column to table i at the source. Later inserts
// into that table fill it.
func (h *FaultHarness) AddExtraColumn(i int) {
	h.t.Helper()
	if !h.w.SchemaChange {
		return
	}
	require.NoError(h.t, h.w.Source.AddExtraColumn(h.t.Context(), h.tables[i].Source))
	h.mu.Lock()
	h.extraAdded[i] = true
	h.mu.Unlock()
}

// Insert inserts one row into table i and waits for it to become its own
// batch. With MaxBatchSize=1 and one insert at a time, the first synced batch
// ID above the previous one is the row's batch. If sync does not advance in
// time the row stays unattributed, and only end-state checks cover it.
func (h *FaultHarness) Insert(i int) {
	h.t.Helper()
	rows, withExtra := h.newRows(i, 1, "")
	h.insertAttributed(i, rows, withExtra, func(ctx context.Context) error {
		return h.w.Source.Insert(ctx, h.tables[i].Source, rows[0], withExtra)
	})
}

// InsertLarge inserts n rows into table i in one statement, each larger than
// FaultLargeRowBytes, so a destination that splits staged files by size gets
// one batch with n files. The rows share one batch, attributed like Insert.
// It reports false, inserting nothing, if the source has no InsertMany.
func (h *FaultHarness) InsertLarge(i, n int) bool {
	h.t.Helper()
	bulk, ok := h.w.Source.(FaultBulkSource)
	if !ok {
		return false
	}
	rows, withExtra := h.newRows(i, n, strings.Repeat("p", FaultLargeRowBytes))
	h.insertAttributed(i, rows, withExtra, func(ctx context.Context) error {
		return bulk.InsertMany(ctx, h.tables[i].Source, rows, withExtra)
	})
	return true
}

func (h *FaultHarness) newRows(i, n int, padding string) ([]FaultRow, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	withExtra := h.extraAdded[i]
	rows := make([]FaultRow, n)
	for j := range rows {
		id := h.nextID
		h.nextID++
		rows[j] = FaultRow{ID: id, Val: fmt.Sprintf("v%d", id)}
		if padding != "" {
			rows[j].Val += "-" + padding
		}
		if withExtra {
			rows[j].Extra = fmt.Sprintf("x%d", id)
		}
	}
	return rows, withExtra
}

// insertAttributed runs insert, which must land rows in one batch, and records
// the rows with the first synced batch above the one before the insert.
func (h *FaultHarness) insertAttributed(i int, rows []FaultRow, withExtra bool, insert func(context.Context) error) {
	h.t.Helper()
	table := h.tables[i]
	trusted := h.attributionTrusted(table.ProgressKey)
	before := h.syncedOrZero(table.ProgressKey)
	require.NoError(h.t, insert(h.t.Context()))

	batch := int64(unattributed)
	h.pollUntil(faultAttribution, func() bool {
		if synced := h.syncedOrZero(table.ProgressKey); synced > before {
			batch = synced
			return true
		}
		return false
	})
	if !trusted {
		batch = unattributed
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if batch == unattributed {
		h.unattributedSinceCaughtUp[table.ProgressKey] = true
	}
	for _, row := range rows {
		h.expected = append(h.expected, expectedRow{row: row, table: i, batch: batch, hasExtra: withExtra})
	}
	if withExtra && h.schemaBatch[i] == unattributed {
		h.schemaBatch[i] = batch
	}
}

// attributionTrusted reports whether the next synced batch must contain a row
// inserted now. That holds only if, since the mirror was last fully caught up,
// no fail site fired, SyncFlow did not restart, and every row with this progress key was
// attributed. Otherwise a late batch of an earlier row, or a replay of
// already-synced rows after a restart, can be the next batch, and the row would
// get a batch ID lower than its real one.
func (h *FaultHarness) attributionTrusted(key string) bool {
	fired, err := h.firedFails()
	if err != nil {
		h.t.Log(err)
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return fired == h.caughtUpFails && !h.restartedSinceCaughtUp && !h.unattributedSinceCaughtUp[key]
}

func (h *FaultHarness) firedFails() (int64, error) {
	return testhooks.FiredFails(h.t.Context(), h.catalog, h.flowName)
}

func (h *FaultHarness) syncedOrZero(key string) int64 {
	p, err := h.w.Progress(h.t.Context(), h.flowName, h.tables)
	if err != nil {
		h.t.Log(err)
		return 0
	}
	return p.Batches[key].Synced
}

// Progress reads the current progress, failing the test on read errors.
func (h *FaultHarness) Progress() FaultProgress {
	h.t.Helper()
	p, err := h.w.Progress(h.t.Context(), h.flowName, h.tables)
	require.NoError(h.t, err)
	return p
}

// WaitProgress waits until cond holds for the current progress.
func (h *FaultHarness) WaitProgress(timeout time.Duration, cond func(FaultProgress) bool) bool {
	return h.pollUntil(timeout, func() bool {
		p, err := h.w.Progress(h.t.Context(), h.flowName, h.tables)
		if err != nil {
			h.t.Log(err)
			return false
		}
		return cond(p)
	})
}

// WaitCaughtUp waits until every progress key has normalized what it synced and
// every destination table matches the inserted rows exactly.
func (h *FaultHarness) WaitCaughtUp(timeout time.Duration) bool {
	return h.pollUntil(timeout, func() bool {
		p, err := h.w.Progress(h.t.Context(), h.flowName, h.tables)
		if err != nil {
			h.t.Log(err)
			return false
		}
		for _, d := range p.Batches {
			if d.Normalized < d.Synced {
				return false
			}
		}
		if h.destinationDiff() != "" {
			return false
		}
		h.markCaughtUp()
		return true
	})
}

// markCaughtUp records that the mirror was fully caught up, so attribution can be trusted again.
func (h *FaultHarness) markCaughtUp() {
	fired, err := h.firedFails()
	if err != nil {
		h.t.Log(err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.caughtUpFails = fired
	h.restartedSinceCaughtUp = false
	clear(h.unattributedSinceCaughtUp)
}

// WaitRowArrives waits until the most recently inserted row of table i is at the destination.
func (h *FaultHarness) WaitRowArrives(i int, timeout time.Duration) bool {
	h.mu.Lock()
	var last *expectedRow
	for j := len(h.expected) - 1; j >= 0; j-- {
		if h.expected[j].table == i {
			last = &h.expected[j]
			break
		}
	}
	h.mu.Unlock()
	if last == nil {
		return true
	}
	return h.pollUntil(timeout, func() bool {
		rows, err := h.w.Dest.Rows(h.t.Context(), h.tables[i].Dest)
		if err != nil {
			return false
		}
		got, ok := rows[last.row.ID]
		return ok && got == last.row
	})
}

// WaitFlowError waits for a flow error whose message contains pattern.
func (h *FaultHarness) WaitFlowError(pattern string, timeout time.Duration) bool {
	return h.pollUntil(timeout, func() bool {
		n, err := GetLogCount(h.t.Context(), shared.CatalogPool{Pool: h.catalog}, h.flowName, "error", pattern)
		return err == nil && n > 0
	})
}

// RunningNormalizes counts normalize statements running against table i.
func (h *FaultHarness) RunningNormalizes(i int) int {
	n, err := h.w.Dest.RunningNormalizes(h.t.Context(), h.tables[i].Dest)
	if err != nil {
		h.t.Log(err)
	}
	return n
}

// RestartSyncFlow pauses and resumes the mirror, which restarts SyncFlow and
// reruns its startup. A pause that never completes is a violation (check 12).
func (h *FaultHarness) RestartSyncFlow() bool {
	h.mu.Lock()
	h.restartedSinceCaughtUp = true
	h.mu.Unlock()
	SignalWorkflow(h.t.Context(), h.env, model.FlowSignal, model.PauseSignal)
	if !h.pollUntil(faultLiveness, func() bool { return h.env.GetFlowStatus(h.t) == protos.FlowStatus_STATUS_PAUSED }) {
		h.Violation("pause", "[28,29] mirror did not reach PAUSED within %s: a worker did not stop", faultLiveness)
		return false
	}
	SignalWorkflow(h.t.Context(), h.env, model.FlowSignal, model.NoopSignal)
	if !h.pollUntil(faultLiveness, func() bool { return h.env.GetFlowStatus(h.t) == protos.FlowStatus_STATUS_RUNNING }) {
		h.Violation("resume", "[28] mirror did not return to RUNNING within %s", faultLiveness)
		return false
	}
	return true
}

// CheckSiblingProgresses checks that table other keeps replicating while its
// sibling is stuck (check 14). It applies only where tables progress independently.
func (h *FaultHarness) CheckSiblingProgresses(other int) {
	if !h.w.IndependentTables {
		return
	}
	h.Insert(other)
	if !h.WaitRowArrives(other, faultLiveness) {
		h.Violation("sibling", "[32,36] table %s stopped replicating while its sibling was stuck", h.tables[other].Source)
	}
}

func (h *FaultHarness) pollUntil(timeout time.Duration, f func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if f() {
			return true
		}
		if time.Now().After(deadline) || h.t.Context().Err() != nil {
			return false
		}
		select {
		case <-h.t.Context().Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

// destinationDiff describes how the destination differs from the inserted rows, or "" if equal.
func (h *FaultHarness) destinationDiff() string {
	h.mu.Lock()
	expected := slices.Clone(h.expected)
	h.mu.Unlock()

	var diffs []string
	for i, table := range h.tables {
		got, err := h.w.Dest.Rows(h.t.Context(), table.Dest)
		if err != nil {
			return err.Error()
		}
		want := map[int64]FaultRow{}
		for _, e := range expected {
			if e.table == i {
				want[e.row.ID] = e.row
			}
		}
		for id, row := range want {
			if g, ok := got[id]; !ok {
				diffs = append(diffs, fmt.Sprintf("%s: row %d missing", table.Dest, id))
			} else if g != row {
				diffs = append(diffs, fmt.Sprintf("%s: row %d is %+v, want %+v", table.Dest, id, g, row))
			}
		}
		for id := range got {
			if _, ok := want[id]; !ok {
				diffs = append(diffs, fmt.Sprintf("%s: unexpected row %d", table.Dest, id))
			}
		}
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "; ")
}

// Finish runs the end-of-case checks, then reports violations. With no
// violations, a case whose armed sites never fired is skipped as not reached.
func (h *FaultHarness) Finish() {
	h.t.Helper()
	for _, release := range h.releases {
		release()
	}

	// [1,8,13,21,22,24,27] Everything synced is eventually normalized, with no new source data.
	caughtUp := h.WaitCaughtUp(faultLiveness)
	if !caughtUp {
		h.Violation("catchup", "[1,8,13,22,24] normalize did not catch up within %s after the source went quiet: %s",
			faultLiveness, h.progressAndDiff())
	} else {
		// [18] A quiet source creates no new batch obligation.
		before := h.Progress()
		time.Sleep(faultQuiet)
		after := h.Progress()
		for key, d := range after.Batches {
			if d.Synced != before.Batches[key].Synced {
				h.Violation("quiet", "[18] %s moved from %d to %d with no new source rows",
					syncedName(key), before.Batches[key].Synced, d.Synced)
			}
		}
	}

	// [9] Sync is not stranded: a new row still arrives.
	h.Insert(0)
	if !h.WaitRowArrives(0, faultLiveness) {
		h.Violation("probe", "[9] a row inserted after the quiet period did not arrive within %s: sync is stuck (%s)",
			faultLiveness, h.progressAndDiff())
	}

	// [28,29] Shutdown releases every waiter; the restarted mirror still works.
	if h.RestartSyncFlow() {
		h.Insert(len(h.tables) - 1)
		if !h.WaitRowArrives(len(h.tables)-1, faultLiveness) {
			h.Violation("probe-restart", "[28] a row inserted after pause and resume did not arrive within %s", faultLiveness)
		}
	}

	// [15,16,20] The end state is exact.
	if !h.WaitCaughtUp(faultLiveness) {
		h.Violation("final", "[15,16,20] destination does not match the source: %s", h.progressAndDiff())
	}

	h.stopMonitor()
	<-h.monitorDone

	// [37] No stale durable progress write was attempted.
	stale, err := catalogStaleWrites(h.t.Context(), h.catalog, h.flowName)
	require.NoError(h.t, err)
	destStale, err := h.w.Dest.StaleWrites(h.t.Context(), h.flowName)
	require.NoError(h.t, err)
	for _, s := range append(stale, destStale...) {
		h.Violation("stale:"+s, "[37] stale durable progress write attempted: %s", s)
	}

	h.report()
}

func (h *FaultHarness) progressAndDiff() string {
	p, err := h.w.Progress(h.t.Context(), h.flowName, h.tables)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%s; diff=%s", progressText(p.Batches), h.destinationDiff())
}

func (h *FaultHarness) report() {
	h.mu.Lock()
	violations := make([]*violation, 0, len(h.violations))
	for _, v := range h.violations {
		violations = append(violations, v)
	}
	h.mu.Unlock()
	sort.Slice(violations, func(i, j int) bool { return violations[i].first.Before(violations[j].first) })
	for _, v := range violations {
		h.t.Errorf("%s (seen %d times, first at %s)", v.msg, v.count, v.first.Format(time.RFC3339))
	}
	if len(violations) > 0 {
		return
	}

	var notReached []string
	for _, a := range h.armed {
		fired, _, err := testhooks.Fired(h.t.Context(), h.catalog, h.flowName, a.Site)
		if err != nil || !fired {
			notReached = append(notReached, a.Site.Name)
		}
	}
	if len(notReached) > 0 {
		h.t.Skipf("NOT REACHED: %s never fired", strings.Join(notReached, ", "))
	}
}

func (h *FaultHarness) cleanup() {
	if h.stopMonitor != nil {
		h.stopMonitor()
	}
	for _, release := range h.releases {
		release()
	}
	if h.env.WorkflowRun != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		h.env.Cancel(ctx)
	}
	h.w.Dest.Close()
}

// monitor samples progress and the destination, checking the properties that
// must hold at every moment.
func (h *FaultHarness) monitor(ctx context.Context) {
	defer close(h.monitorDone)
	ticker := time.NewTicker(faultSample)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.sample(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				h.t.Logf("monitor sample failed: %v", err)
			}
		}
	}
}

func (h *FaultHarness) sample(ctx context.Context) error {
	// Read order matters: progress first, destination second. The destination
	// can only be ahead of the progress read, never behind it.
	p, err := h.w.Progress(ctx, h.flowName, h.tables)
	if err != nil {
		return err
	}
	srcDurable, err := h.w.Source.Durable(ctx, h.flowName)
	if err != nil {
		return err
	}
	destRows := make([]map[int64]FaultRow, len(h.tables))
	for i, table := range h.tables {
		if destRows[i], err = h.w.Dest.Rows(ctx, table.Dest); err != nil {
			return err
		}
	}

	h.mu.Lock()
	expected := slices.Clone(h.expected)
	schemaBatch := slices.Clone(h.schemaBatch)
	h.mu.Unlock()

	// [6,23] Durable progress never decreases.
	h.checkMonotone(p.Durable)
	h.checkMonotone(srcDurable)

	for key, d := range p.Batches {
		// [34] Completion never exceeds the request.
		if d.Normalized > d.Synced {
			h.Violation("ahead:"+key, "[34] %s %d is ahead of %s %d",
				normalizedName(key), d.Normalized, syncedName(key), d.Synced)
		}
		// [10,35] Backpressure holds the pending gap at the buffer.
		if gap := d.Synced - d.Normalized; gap > h.w.BufferLimit {
			h.Violation("buffer:"+key, "[10,35] %s has %d batches synced but not normalized, limit %d",
				scopeName(key), gap, h.w.BufferLimit)
		}
	}

	// [2,3,4,5,7,14,33] Every row in an acknowledged batch is applied, with the right values.
	for _, e := range expected {
		key := h.tables[e.table].ProgressKey
		if e.batch > p.Batches[key].Normalized {
			continue
		}
		got, ok := destRows[e.table][e.row.ID]
		switch {
		case !ok:
			h.Violation(fmt.Sprintf("ack:%d", e.row.ID),
				"[2,4,14] batch %d is acknowledged (%s=%d) but row %d of %s is missing",
				e.batch, normalizedName(key), p.Batches[key].Normalized, e.row.ID, h.tables[e.table].Dest)
		case got != e.row:
			h.Violation(fmt.Sprintf("ackval:%d", e.row.ID),
				"[5,33] batch %d is acknowledged but row %d of %s is %+v, want %+v",
				e.batch, e.row.ID, h.tables[e.table].Dest, got, e.row)
		}
	}

	// [5,25] Once a schema-change batch is acknowledged, both schemas have the column.
	for i, batch := range schemaBatch {
		if batch == unattributed || batch > p.Batches[h.tables[i].ProgressKey].Normalized {
			continue
		}
		if ok, err := h.w.Dest.HasExtraColumn(ctx, h.tables[i].Dest); err == nil && !ok {
			h.Violation(fmt.Sprintf("destschema:%d", i), "[5,25] schema batch %d of %s is acknowledged without the destination column",
				batch, h.tables[i].Dest)
		}
		if ok, err := catalogHasExtraColumn(ctx, h.catalog, h.flowName, h.tables[i].CatalogName); err == nil && !ok {
			h.Violation(fmt.Sprintf("catschema:%d", i), "[5,25] schema batch %d of %s is acknowledged without the catalog column",
				batch, h.tables[i].CatalogName)
		}
	}

	// [31] At most one normalize writes a table at a time.
	for i, table := range h.tables {
		if n, err := h.w.Dest.RunningNormalizes(ctx, table.Dest); err == nil && n > 1 {
			h.Violation(fmt.Sprintf("overlap:%d", i), "[31] %d normalize statements are writing %s at once", n, table.Dest)
		}
	}

	// [17,19] A published batch without a stage row must already be applied.
	for key, batches := range p.MissingStages {
		for _, b := range batches {
			for _, e := range expected {
				if e.batch != b || h.tables[e.table].ProgressKey != key {
					continue
				}
				if _, ok := destRows[e.table][e.row.ID]; !ok {
					h.Violation(fmt.Sprintf("stage:%s:%d", key, b),
						"[17,19] batch %d of %s has no stage row but row %d was never applied", b, scopeName(key), e.row.ID)
				}
			}
		}
	}

	// [3] A table's watermark covers every acknowledged batch that touched it.
	if p.TableWatermarks != nil {
		for i, table := range h.tables {
			var need int64
			for _, e := range expected {
				if e.table == i && e.batch <= p.Batches[table.ProgressKey].Normalized {
					need = max(need, e.batch)
				}
			}
			if got := p.TableWatermarks[table.Dest]; got < need {
				h.Violation(fmt.Sprintf("watermark:%d", i), "[3] table watermark of %s is %d, behind acknowledged batch %d",
					table.Dest, got, need)
			}
		}
	}
	return nil
}

func (h *FaultHarness) checkMonotone(values map[string]int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, v := range values {
		if prev, ok := h.durableMax[k]; ok && v < prev {
			key := "monotone:" + k
			if _, seen := h.violations[key]; !seen {
				msg := fmt.Sprintf("[6,23,37] durable progress %s went from %d to %d", k, prev, v)
				h.violations[key] = &violation{first: time.Now(), msg: msg, count: 1}
				h.t.Logf("VIOLATION %s: %s", key, msg)
			}
			continue
		}
		h.durableMax[k] = v
	}
}

func catalogHasExtraColumn(ctx context.Context, catalog *pgxpool.Pool, flowName string, table string) (bool, error) {
	schemas, err := internal.LoadTableSchemasFromCatalog(ctx, catalog, flowName, []string{table})
	if err != nil {
		return false, err
	}
	schema, ok := schemas[table]
	if !ok {
		return false, fmt.Errorf("no catalog schema for %s", table)
	}
	for _, col := range schema.Columns {
		if col.Name == FaultExtraColumn {
			return true, nil
		}
	}
	return false, nil
}

// syncedName and normalizedName name a progress value as its catalog column does.
func syncedName(key string) string {
	if key == ProgressMirror {
		return "sync_batch_id"
	}
	return "synced_batch_id of table " + shortTable(key)
}

func normalizedName(key string) string {
	if key == ProgressMirror {
		return "normalize_batch_id"
	}
	return "normalized_batch_id of table " + shortTable(key)
}

func scopeName(key string) string {
	if key == ProgressMirror {
		return "the mirror"
	}
	return "table " + shortTable(key)
}

// shortTable drops the dataset or schema prefix from a source table identifier.
func shortTable(key string) string {
	return key[strings.LastIndex(key, ".")+1:]
}

// progressText prints batch progress with the catalog's column names.
func progressText(batches map[string]BatchProgress) string {
	if b, ok := batches[ProgressMirror]; ok && len(batches) == 1 {
		return fmt.Sprintf("sync_batch_id=%d, normalize_batch_id=%d", b.Synced, b.Normalized)
	}
	keys := slices.Sorted(maps.Keys(batches))
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		b := batches[key]
		parts = append(parts, fmt.Sprintf("table %s: synced_batch_id=%d, normalized_batch_id=%d",
			shortTable(key), b.Synced, b.Normalized))
	}
	return strings.Join(parts, "; ")
}
