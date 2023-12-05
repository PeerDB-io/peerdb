import { FlowConnectionConfigs, QRepSyncMode } from '@/grpc_generated/flow';

const syncModeToLabel = (mode: QRepSyncMode) => {
  switch (mode.toString()) {
    case 'QREP_SYNC_MODE_STORAGE_AVRO':
      return 'AVRO';
    case 'QREP_SYNC_MODE_MULTI_INSERT':
      return 'Copy with Binary';
    default:
      return 'AVRO';
  }
};
const MirrorValues = (mirrorConfig: FlowConnectionConfigs | undefined) => {
  return [
    {
      value: `${mirrorConfig?.maxBatchSize} rows`,
      label: 'Pull Batch Size',
    },
    {
      value: `${mirrorConfig?.snapshotNumRowsPerPartition} rows`,
      label: 'Snapshot Rows Per Partition',
    },
    {
      value: `${mirrorConfig?.snapshotNumTablesInParallel} table(s)`,
      label: 'Snapshot Tables In Parallel',
    },
    {
      value: `${mirrorConfig?.snapshotMaxParallelWorkers} worker(s)`,
      label: 'Snapshot Parallel Tables',
    },
    {
      value: `${syncModeToLabel(mirrorConfig?.cdcSyncMode!)}`,
      label: 'CDC Sync Mode',
    },
    {
      value: `${syncModeToLabel(mirrorConfig?.snapshotSyncMode!)}`,
      label: 'Snapshot Sync Mode',
    },
    {
      value: `${
        mirrorConfig?.cdcStagingPath?.length
          ? mirrorConfig?.cdcStagingPath
          : 'Local'
      }`,
      label: 'CDC Staging Path',
    },
    {
      value: `${
        mirrorConfig?.snapshotStagingPath?.length
          ? mirrorConfig?.snapshotStagingPath
          : 'Local'
      }`,
      label: 'Snapshot Staging Path',
    },
  ];
};
export default MirrorValues;
