import { FlowConnectionConfigs } from '@/grpc_generated/flow';

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
