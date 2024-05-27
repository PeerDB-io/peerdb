// These are the backend defaults if the user creates a new mirror
// via SQL layer without specifying these settings.
const DefaultSyncInterval = 60;
const DefaultSnapshotNumRowsPerPartition = 1000000;
const DefaultPullBatchSize = 1000000;
const DefaultSnapshotNumTablesInParallel = 1;
const DefaultSnapshotMaxParallelWorkers = 4;

export {
  DefaultPullBatchSize,
  DefaultSnapshotMaxParallelWorkers,
  DefaultSnapshotNumRowsPerPartition,
  DefaultSnapshotNumTablesInParallel,
  DefaultSyncInterval,
};
