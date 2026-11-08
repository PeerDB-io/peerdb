package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
)

// Outcome is what a known failure is expected to show on a code version.
type Outcome string

const (
	OutcomeRed   Outcome = "red"
	OutcomeGreen Outcome = "green"
)

// KnownFailure reproduces one bug from the PR descriptions on real code.
// Name is the PR description's section title. The test only runs the checks;
// OnMain and OnBranch record what each version should show.
type KnownFailure struct {
	Run       func(h *FaultHarness)
	ID        string
	Name      string
	OnMain    Outcome
	OnBranch  Outcome
	Workloads testhooks.Workload
}

const (
	chCDCWorkloads = testhooks.WorkloadPGToCH
	cdcWorkloads   = testhooks.WorkloadPGToPG | chCDCWorkloads
	// The one-minute normalize retry backoff decides these sleeps.
	normalizeRetryBackoff = time.Minute
)

var KnownFailures = []KnownFailure{
	{
		ID: "missed_req", Name: "Normalize misses the final request and leaves data pending", Workloads: cdcWorkloads,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runMissedRequest,
	},
	{
		ID: "q_missed_req", Name: "Normalize misses the final request and leaves data pending", Workloads: testhooks.WorkloadBQToCH,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runMissedRequest,
	},
	{
		ID: "missed_done", Name: "CDC sync keeps waiting after normalization has caught up", Workloads: cdcWorkloads,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runMissedCompletion,
	},
	{
		ID: "q_startup", Name: "Query startup replaces a newer request with an older one", Workloads: testhooks.WorkloadBQToCH,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runQueryStartupOverwrite,
	},
	{
		ID: "ch_nojoin", Name: "NormalizeRecords returns while INSERT workers still run", Workloads: chCDCWorkloads,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runReturnWithoutJoin,
	},
	{
		ID: "ch_overlap", Name: "Retry starts while an old INSERT is still running", Workloads: chCDCWorkloads,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runRetryOverlap,
	},
	{
		ID: "ch_old_ckpt", Name: "An old INSERT worker moves saved progress backward", Workloads: chCDCWorkloads,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runOldInsertOverwritesCheckpoint,
	},
	{
		ID: "pg_ddl", Name: "PostgreSQL marks a batch synced before updating its tables", Workloads: testhooks.WorkloadPGToPG,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: schemaGap(testhooks.PGSyncCommitAfter),
	},
	{
		ID: "pg_catalog", Name: "PostgreSQL restarts with an old catalog column list", Workloads: testhooks.WorkloadPGToPG,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: schemaGap(testhooks.PGSyncDDLAfter),
	},
	{
		ID: "ch_catalog", Name: "ClickHouse restarts with an old catalog column list", Workloads: chCDCWorkloads,
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: schemaGap(testhooks.CHSyncFinishBatchAfter),
	},
	{
		ID: "buffer_fill", Name: "Both loops sleep after the whole buffer fills", Workloads: chCDCWorkloads,
		// Classified as unrealistic in production, where the buffer is thousands of batches.
		OnMain: OutcomeRed, OnBranch: OutcomeGreen, Run: runBufferFillInGap,
	},
}

// RunKnownFailure runs the known failure with the given ID on workload w.
func RunKnownFailure(w FaultWorkload, id string) {
	t := w.Suite.T()
	t.Helper()
	for _, k := range KnownFailures {
		if k.ID != id {
			continue
		}
		if k.Workloads&w.Kind == 0 {
			t.Fatalf("known failure %s does not apply to %s", id, w.Kind)
		}
		t.Logf("known failure %q (%s): expected %s on main, %s on the branch", k.ID, k.Name, k.OnMain, k.OnBranch)
		h := NewFaultHarness(w, id)
		k.Run(h)
		return
	}
	t.Fatalf("no known failure %s", id)
}

// normalizeIdleGap is the normalize loop's check-then-subscribe gap for the workload.
func normalizeIdleGap(h *FaultHarness) (testhooks.Site, string) {
	if h.w.Kind == testhooks.WorkloadBQToCH {
		return testhooks.QCDCNormalizeIdleGap, h.Table(0).HookTable
	}
	return testhooks.CDCNormalizeIdleGap, ""
}

// runMissedRequest: normalize checks for work, then sync publishes before
// normalize subscribes. The request is lost and a quiet source never resends it.
func runMissedRequest(h *FaultHarness) {
	gap, table := normalizeIdleGap(h)
	h.Arm(gap, nil, table)
	// Normalize reaches its idle gap only after finishing work.
	h.Insert(0)
	if h.WaitFired(gap, faultLiveness) {
		before := h.synced()
		h.Insert(0)
		// The request must be published while normalize holds the gap.
		// Query CDC can take longer than Insert waits to deliver a row.
		h.waitSyncedPast(before)
		h.CheckSiblingProgresses(1)
		h.Release(gap)
	}
	h.Finish()
}

// synced returns table 0's synced batch ID. Table 0 is the table the scenarios
// and drivers insert into to race a hold.
func (h *FaultHarness) synced() int64 {
	return h.Progress().Batches[h.Table(0).ProgressKey].Synced
}

// waitSyncedPast waits until table 0's synced batch ID passes after, or the liveness timeout.
func (h *FaultHarness) waitSyncedPast(after int64) {
	key := h.Table(0).ProgressKey
	h.WaitProgress(faultLiveness, func(p FaultProgress) bool { return p.Batches[key].Synced > after })
}

// runMissedCompletion: sync checks backpressure, then normalize finishes
// everything before sync subscribes. Sync sleeps although nothing is pending.
func runMissedCompletion(h *FaultHarness) {
	release := h.Throttle(0, 20*time.Second)
	h.Arm(testhooks.CDCSyncBackpressureGap, nil, "")
	for range FaultBuffer + 1 {
		h.Insert(0)
	}
	if h.WaitFired(testhooks.CDCSyncBackpressureGap, faultLiveness) {
		release()
		h.WaitProgress(faultLiveness, func(p FaultProgress) bool {
			d := p.Batches[ProgressMirror]
			return d.Normalized >= d.Synced
		})
		h.Release(testhooks.CDCSyncBackpressureGap)
	}
	h.Finish()
}

// runQueryStartupOverwrite: a restarted table loop snapshots its state, sync
// publishes a newer batch, then the snapshot overwrites that newer request.
func runQueryStartupOverwrite(h *FaultHarness) {
	// Longer than the waits below, so normalize cannot finish the pending batch
	// before the restart cancels it.
	release := h.Throttle(0, 2*faultLiveness)
	key := h.Table(0).ProgressKey
	before := h.synced()
	h.Insert(0)
	// The snapshot must see this batch saved and not yet normalized.
	h.WaitProgress(faultLiveness, func(p FaultProgress) bool {
		d := p.Batches[key]
		return d.Synced > before && d.Normalized < d.Synced
	})
	h.Arm(testhooks.QCDCNormalizeStartupGap, nil, h.Table(0).HookTable)
	h.RestartSyncFlow()
	if h.WaitFired(testhooks.QCDCNormalizeStartupGap, faultLiveness) {
		release()
		// The canceled normalize's INSERT can keep sleeping on the server.
		h.killNormalizes(0)
		pending := h.synced()
		h.Insert(0)
		// Sync must publish the newer batch while the snapshot is held. The paused
		// loop is the only one normalizing this table, so the pending batch stays.
		h.waitSyncedPast(pending)
		h.CheckSiblingProgresses(1)
		h.Release(testhooks.QCDCNormalizeStartupGap)
	}
	h.Finish()
}

// schemaGap fails sync at site on the batch that carries a schema change. The
// restart then exposes that batch before the schema it needs is in place.
func schemaGap(site testhooks.Site) func(h *FaultHarness) {
	return func(h *FaultHarness) {
		h.Arm(site, nil, "")
		h.AddExtraColumn(0)
		h.Insert(0)
		h.WaitFired(site, faultLiveness)
		// Later batches, after the restart.
		h.Insert(0)
		h.Insert(1)
		h.Finish()
	}
}

// startSlowInsertThenFailProducer leaves table 0's INSERT sleeping, then fails
// the producer after it handed that INSERT to a worker.
func startSlowInsertThenFailProducer(h *FaultHarness, slowness time.Duration) (func(), bool) {
	release := h.Throttle(0, slowness)
	h.Arm(testhooks.CHNormalizeEnqueueAfter, nil, "")
	h.Insert(0)
	return release, h.WaitFired(testhooks.CHNormalizeEnqueueAfter, faultLiveness)
}

func (h *FaultHarness) killNormalizes(i int) {
	if ch, ok := h.w.Dest.(*ChFaultDestination); ok {
		if err := ch.KillInserts(h.t.Context(), h.tables[i].Dest); err != nil {
			h.t.Log(err)
		}
	}
}

// holdInsertWorkerThenFailProducer holds table 0's INSERT worker after its
// INSERT returns, then fails the producer. The checks look at the worker, not at
// the ClickHouse server, which can keep running a canceled INSERT.
func holdInsertWorkerThenFailProducer(h *FaultHarness) bool {
	h.Arm(testhooks.CHNormalizeInsertReturnGap, nil, "")
	h.Arm(testhooks.CHNormalizeEnqueueAfter, nil, "")
	h.Insert(0)
	return h.WaitFired(testhooks.CHNormalizeEnqueueAfter, faultLiveness) &&
		h.WaitFired(testhooks.CHNormalizeInsertReturnGap, faultLiveness)
}

// runReturnWithoutJoin: NormalizeRecords returns its error while an INSERT
// worker is still running.
func runReturnWithoutJoin(h *FaultHarness) {
	if holdInsertWorkerThenFailProducer(h) {
		if h.WaitFlowError(testhooks.CHNormalizeEnqueueAfter.Name, time.Minute) {
			h.Violation("join", "[29] NormalizeRecords reported its error while an INSERT worker into %s was still running",
				h.Table(0).Dest)
		}
		h.Release(testhooks.CHNormalizeInsertReturnGap)
	}
	h.Finish()
}

// runRetryOverlap: the retry after the one-minute backoff starts while the old
// attempt's INSERT worker is still held.
func runRetryOverlap(h *FaultHarness) {
	// Claimable only after the old worker is held, so only a retry can fire it.
	h.Arm(testhooks.CHNormalizeCopyStagesAfter, &testhooks.CHNormalizeInsertReturnGap, "")
	if holdInsertWorkerThenFailProducer(h) {
		if h.WaitFired(testhooks.CHNormalizeCopyStagesAfter, normalizeRetryBackoff+30*time.Second) {
			h.Violation("retry_overlap", "[31] a retry started while the previous attempt's INSERT worker into %s was still running",
				h.Table(0).Dest)
		}
		h.Release(testhooks.CHNormalizeInsertReturnGap)
	}
	h.Finish()
}

// runOldInsertOverwritesCheckpoint: the retry finishes a newer target first,
// then the old INSERT finishes and writes its older table checkpoint.
func runOldInsertOverwritesCheckpoint(h *FaultHarness) {
	const slowness = 2 * time.Minute
	release, fired := startSlowInsertThenFailProducer(h, slowness)
	// Without the throttle the retry's INSERT is fast; the old one keeps sleeping.
	release()
	if fired {
		// The retry targets this newer batch.
		h.Insert(0)
		// The old INSERT finishes after the retry and writes its checkpoint last.
		time.Sleep(slowness + 15*time.Second)
	}
	h.Finish()
}

// runBufferFillInGap: normalize misses every request while sync fills the
// buffer, so both loops wait on each other.
func runBufferFillInGap(h *FaultHarness) {
	h.Arm(testhooks.CDCNormalizeIdleGap, nil, "")
	h.Insert(0)
	if h.WaitFired(testhooks.CDCNormalizeIdleGap, faultLiveness) {
		for range FaultBuffer + 1 {
			h.Insert(1)
		}
		h.Release(testhooks.CDCNormalizeIdleGap)
	}
	h.Finish()
}

// FaultWorkload adapts this suite's Postgres source and ClickHouse destination.
func (s ClickHouseSuite) FaultWorkload() FaultWorkload {
	t := s.t
	t.Helper()
	if s.cluster {
		t.Skip("fault tests run once, on the non-cluster suite")
	}
	pgSource, ok := s.source.(*PostgresSource)
	if !ok {
		t.Skip("fault tests run on the Postgres source only")
	}
	ctx := t.Context()
	source, err := NewPgFaultSource(ctx, pgSource.Conn(), Schema(s))
	require.NoError(t, err)
	t.Cleanup(source.Pool.Close)
	dest, err := NewChFaultDestination(ctx, s.Peer())
	require.NoError(t, err)
	return FaultWorkload{
		Suite:    s,
		Source:   source,
		Dest:     dest,
		Progress: CatalogCDCProgress(s.catalog.Pool),
		Tables: func(id string) []FaultTable {
			var tables []FaultTable
			for _, suffix := range []string{"a", "b"} {
				name := fmt.Sprintf("fault_%s_%s", id, suffix)
				tables = append(tables, FaultTable{
					Source: name, Dest: name + "_dst", CatalogName: name + "_dst", ProgressKey: ProgressMirror,
				})
			}
			return tables
		},
		FlowConfig: func(flowName string, tables []FaultTable) *protos.FlowConnectionConfigs {
			mapping := map[string]string{}
			for _, table := range tables {
				mapping[AttachSchema(s, table.Source)] = table.Dest
			}
			gen := FlowConnectionGenerationConfig{
				FlowJobName:      flowName,
				TableNameMapping: mapping,
				Destination:      s.Peer().Name,
			}
			config := gen.GenerateFlowConnectionConfigs(s)
			config.MaxBatchSize = 1
			config.IdleTimeoutSeconds = 1
			return config
		},
		Kind:         testhooks.WorkloadPGToCH,
		BufferLimit:  FaultBuffer + 1,
		SchemaChange: true,
	}
}

func (s ClickHouseSuite) Test_FaultKnown_NormalizeMissesTheFinalRequestAndLeavesDataPending() {
	RunKnownFailure(s.FaultWorkload(), "missed_req")
}

func (s ClickHouseSuite) Test_FaultKnown_CDCSyncKeepsWaitingAfterNormalizationHasCaughtUp() {
	RunKnownFailure(s.FaultWorkload(), "missed_done")
}

func (s ClickHouseSuite) Test_FaultKnown_ClickHouseRestartsWithAnOldCatalogColumnList() {
	RunKnownFailure(s.FaultWorkload(), "ch_catalog")
}

func (s ClickHouseSuite) Test_FaultKnown_NormalizeRecordsReturnsWhileINSERTWorkersStillRun() {
	RunKnownFailure(s.FaultWorkload(), "ch_nojoin")
}

func (s ClickHouseSuite) Test_FaultKnown_RetryStartsWhileAnOldINSERTIsStillRunning() {
	RunKnownFailure(s.FaultWorkload(), "ch_overlap")
}

func (s ClickHouseSuite) Test_FaultKnown_AnOldINSERTWorkerMovesSavedProgressBackward() {
	RunKnownFailure(s.FaultWorkload(), "ch_old_ckpt")
}

func (s ClickHouseSuite) Test_FaultKnown_BothLoopsSleepAfterTheWholeBufferFills() {
	RunKnownFailure(s.FaultWorkload(), "buffer_fill")
}
