package testhooks

// Kind decides how a site reacts once armed.
type Kind int

const (
	// KindFail sites sit in a gap between two durable progress writes.
	// An armed site returns an error once, which exercises recovery from that gap.
	KindFail Kind = iota
	// KindPause sites sit in a check-then-act gap on in-memory progress.
	// An armed site blocks until the test releases it.
	KindPause
)

func (k Kind) String() string {
	if k == KindPause {
		return "pause"
	}
	return "fail"
}

type Loop string

const (
	LoopStartup   Loop = "startup"
	LoopSync      Loop = "sync"
	LoopNormalize Loop = "normalize"
)

type Workload uint8

const (
	WorkloadPGToPG Workload = 1 << iota
	WorkloadPGToCH
	WorkloadBQToCH

	workloadsCDC = WorkloadPGToPG | WorkloadPGToCH
)

var Workloads = []Workload{WorkloadPGToPG, WorkloadPGToCH, WorkloadBQToCH}

func (w Workload) String() string {
	switch w {
	case WorkloadPGToPG:
		return "pg_to_pg"
	case WorkloadPGToCH:
		return "pg_to_ch"
	case WorkloadBQToCH:
		return "bq_to_ch"
	}
	return "mixed"
}

// Site is one hook location. Names are shared by every code version under test,
// so the same name must mean the same protocol step in each version.
type Site struct {
	Name      string
	Loop      Loop
	Kind      Kind
	Workloads Workload
	// SchemaChangeOnly sites are reached only by batches that carry schema changes.
	SchemaChangeOnly bool
}

func (s Site) In(w Workload) bool {
	return s.Workloads&w != 0
}

var (
	CDCStartupAfter = Site{Name: "cdc.startup.after", Kind: KindFail, Loop: LoopStartup, Workloads: workloadsCDC}

	CDCSyncSourceOffsetAfter = Site{
		Name: "cdc.sync.source_offset.after", Kind: KindFail, Loop: LoopSync, Workloads: workloadsCDC,
	}
	CDCSyncCatalogSchemaAfter = Site{
		Name: "cdc.sync.catalog_schema.after", Kind: KindFail, Loop: LoopSync, Workloads: workloadsCDC, SchemaChangeOnly: true,
	}
	CDCSyncPublishSyncedAfter = Site{
		Name: "cdc.sync.publish_synced.after", Kind: KindFail, Loop: LoopSync, Workloads: workloadsCDC,
	}
	CDCSyncBackpressureGap = Site{
		Name: "cdc.sync.backpressure.gap", Kind: KindPause, Loop: LoopSync, Workloads: workloadsCDC,
	}
	// No current CDC connector delivers a batch with a schema change and no rows,
	// so these sites are not reached yet.
	CDCSchemaOnlyDDLAfter = Site{
		Name: "cdc.schema_only.ddl.after", Kind: KindFail, Loop: LoopSync, Workloads: workloadsCDC, SchemaChangeOnly: true,
	}
	CDCSchemaOnlyCatalogSchemaAfter = Site{
		Name: "cdc.schema_only.catalog_schema.after", Kind: KindFail, Loop: LoopSync, Workloads: workloadsCDC,
		SchemaChangeOnly: true,
	}
	CDCNormalizeIdleGap = Site{
		Name: "cdc.normalize.idle.gap", Kind: KindPause, Loop: LoopNormalize, Workloads: workloadsCDC,
	}
	CDCNormalizePublishNormalizedBefore = Site{
		Name: "cdc.normalize.publish_normalized.before", Kind: KindFail, Loop: LoopNormalize, Workloads: workloadsCDC,
	}
	CDCNormalizePublishNormalizedAfter = Site{
		Name: "cdc.normalize.publish_normalized.after", Kind: KindFail, Loop: LoopNormalize, Workloads: workloadsCDC,
	}

	PGSyncCommitBefore = Site{Name: "pg.sync.commit.before", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToPG}
	PGSyncCommitAfter  = Site{Name: "pg.sync.commit.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToPG}
	PGSyncDDLAfter     = Site{
		Name: "pg.sync.ddl.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToPG, SchemaChangeOnly: true,
	}
	PGNormalizeCommitBefore = Site{
		Name: "pg.normalize.commit.before", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToPG,
	}
	PGNormalizeCommitAfter = Site{
		Name: "pg.normalize.commit.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToPG,
	}

	CHSyncRawStageBefore = Site{Name: "ch.sync.raw_stage.before", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToCH}
	CHSyncRawStageAfter  = Site{Name: "ch.sync.raw_stage.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToCH}
	CHSyncDDLAfter       = Site{
		Name: "ch.sync.ddl.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToCH, SchemaChangeOnly: true,
	}
	CHSyncFinishBatchAfter = Site{
		Name: "ch.sync.finish_batch.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadPGToCH,
	}
	CHNormalizeCopyStagesAfter = Site{
		Name: "ch.normalize.copy_stages.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToCH,
	}
	CHNormalizeEnqueueAfter = Site{
		Name: "ch.normalize.enqueue.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToCH,
	}
	CHNormalizeInsertAfter = Site{
		Name: "ch.normalize.insert.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToCH,
	}
	CHNormalizeTableBatchIDAfter = Site{
		Name: "ch.normalize.table_batch_id.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToCH,
	}
	CHNormalizeBatchIDBefore = Site{
		Name: "ch.normalize.batch_id.before", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadPGToCH,
	}

	QCDCSyncStageFilesAfter = Site{
		Name: "qcdc.sync.stage_files.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadBQToCH,
	}
	QCDCSyncSetStageAfter      = Site{Name: "qcdc.sync.set_stage.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadBQToCH}
	QCDCSyncRecordAfter        = Site{Name: "qcdc.sync.record.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadBQToCH}
	QCDCSyncPublishSyncedAfter = Site{
		Name: "qcdc.sync.publish_synced.after", Kind: KindFail, Loop: LoopSync, Workloads: WorkloadBQToCH,
	}
	QCDCSyncBackpressureGap = Site{
		Name: "qcdc.sync.backpressure.gap", Kind: KindPause, Loop: LoopSync, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeStartupGap = Site{
		Name: "qcdc.normalize.startup.gap", Kind: KindPause, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeIdleGap = Site{
		Name: "qcdc.normalize.idle.gap", Kind: KindPause, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeInsertFileAfter = Site{
		Name: "qcdc.normalize.insert_file.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeFileCleanupAfter = Site{
		Name: "qcdc.normalize.file_cleanup.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeStageCheckpointAfter = Site{
		Name: "qcdc.normalize.stage_checkpoint.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeDeleteStageAfter = Site{
		Name: "qcdc.normalize.delete_stage.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizeRecordAfter = Site{
		Name: "qcdc.normalize.record.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
	QCDCNormalizePublishNormalizedAfter = Site{
		Name: "qcdc.normalize.publish_normalized.after", Kind: KindFail, Loop: LoopNormalize, Workloads: WorkloadBQToCH,
	}
)

// Sites lists every hook site. TestSitesMatchCallSites keeps it in sync with the code.
var Sites = []Site{
	CDCStartupAfter,
	CDCSyncSourceOffsetAfter,
	CDCSyncCatalogSchemaAfter,
	CDCSyncPublishSyncedAfter,
	CDCSyncBackpressureGap,
	CDCSchemaOnlyDDLAfter,
	CDCSchemaOnlyCatalogSchemaAfter,
	CDCNormalizeIdleGap,
	CDCNormalizePublishNormalizedBefore,
	CDCNormalizePublishNormalizedAfter,

	PGSyncCommitBefore,
	PGSyncCommitAfter,
	PGSyncDDLAfter,
	PGNormalizeCommitBefore,
	PGNormalizeCommitAfter,

	CHSyncRawStageBefore,
	CHSyncRawStageAfter,
	CHSyncDDLAfter,
	CHSyncFinishBatchAfter,
	CHNormalizeCopyStagesAfter,
	CHNormalizeEnqueueAfter,
	CHNormalizeInsertAfter,
	CHNormalizeTableBatchIDAfter,
	CHNormalizeBatchIDBefore,

	QCDCSyncStageFilesAfter,
	QCDCSyncSetStageAfter,
	QCDCSyncRecordAfter,
	QCDCSyncPublishSyncedAfter,
	QCDCSyncBackpressureGap,
	QCDCNormalizeStartupGap,
	QCDCNormalizeIdleGap,
	QCDCNormalizeInsertFileAfter,
	QCDCNormalizeFileCleanupAfter,
	QCDCNormalizeStageCheckpointAfter,
	QCDCNormalizeDeleteStageAfter,
	QCDCNormalizeRecordAfter,
	QCDCNormalizePublishNormalizedAfter,
}
