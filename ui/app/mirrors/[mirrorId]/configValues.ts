import { FlowConnectionConfigs } from '@/grpc_generated/flow';

export default function MirrorValues(
  mirrorConfig: FlowConnectionConfigs | undefined
) {
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
      label: 'Snapshot Parallel Workers',
    },
    {
      value: mirrorConfig?.softDeleteColName
        ? `Enabled (${mirrorConfig?.softDeleteColName})`
        : 'Disabled',
      label: 'Soft Delete',
    },
    {
      value: mirrorConfig?.script,
      label: 'Script',
    },
    {
      value:
        mirrorConfig?.publicationName ||
        `peerflow_pub_${mirrorConfig?.flowJobName}`,
      label: 'Publication Name',
    },
  ];
}
