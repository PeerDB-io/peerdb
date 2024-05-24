import {
  DefaultPullBatchSize,
  DefaultSnapshotNumRowsPerPartition,
} from '@/app/utils/defaultMirrorSettings';
import { FlowConnectionConfigs } from '@/grpc_generated/flow';

const MirrorValues = (mirrorConfig: FlowConnectionConfigs | undefined) => {
  return [
    {
      value: `${mirrorConfig?.maxBatchSize || DefaultPullBatchSize} rows`,
      label: 'Pull Batch Size',
    },
    {
      value: `${mirrorConfig?.snapshotNumRowsPerPartition || DefaultSnapshotNumRowsPerPartition} rows`,
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
      value: `${mirrorConfig?.softDelete}`,
      label: 'Soft Delete',
    },
    {
      value:
        mirrorConfig?.publicationName ||
        'peerflow_pub_' + mirrorConfig?.flowJobName.toLowerCase(),
      label: 'Publication Name',
    },
  ];
};
export default MirrorValues;
