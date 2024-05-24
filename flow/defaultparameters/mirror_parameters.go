package peerdb_mirror_defaults

const (
	// --- INITIAL LOAD ---
	DefaultSnapshotNumTablesInParallel = 1
	DefaultSnapshotParallelWorkers     = 4
	DefaultSnapshotNumRowsPerPartition = 1000000

	// --- CDC ---
	DefaultPullBatchSize = 1000000
)
