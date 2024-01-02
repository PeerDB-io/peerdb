import { CDCConfig } from '../../../dto/MirrorsDTO';
import { MirrorSetting } from './common';
export const cdcSettings: MirrorSetting[] = [
  {
    label: 'Initial Copy',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        doInitialCopy: (value as boolean) ?? true,
      })),
    tips: 'Specify if you want initial load to happen for your tables.',
    type: 'switch',
    default: true,
  },
  {
    label: 'Pull Batch Size',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        maxBatchSize: (value as number) || 100000,
      })),
    tips: 'The number of rows PeerDB will pull from source at a time. If left empty, the default value is 100,000 rows.',
    type: 'number',
    default: '100000',
    advanced: true,
  },
  {
    label: 'Idle Timeout (Seconds)',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        idleTimeoutSeconds: (value as number) || 100000,
      })),
    tips: 'Time after which a Sync flow ends, if it happens before pull batch size is reached. Defaults to 60 seconds.',
    helpfulLink: 'https://docs.peerdb.io/metrics/important_cdc_configs',
    type: 'number',
    default: '60',
  },
  {
    label: 'Publication Name',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        publicationName: (value as string) || '',
      })),
    tips: 'If set, PeerDB will use this publication for the mirror.',
  },
  {
    label: 'Replication Slot Name',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        replicationSlotName: (value as string) || '',
      })),
    tips: 'If set, PeerDB will use this slot for the mirror.',
  },
  {
    label: 'Snapshot Number of Rows Per Partition',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        snapshotNumRowsPerPartition: parseInt(value as string, 10) || 500000,
      })),
    tips: 'PeerDB splits up table data into partitions for increased performance. This setting controls the number of rows per partition. The default value is 500000.',
    default: '500000',
    type: 'number',
  },
  {
    label: 'Snapshot Maximum Parallel Workers',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        snapshotMaxParallelWorkers: parseInt(value as string, 10) || 1,
      })),
    tips: 'PeerDB spins up parallel threads for each partition. This setting controls the number of partitions to sync in parallel. The default value is 1.',
    default: '1',
    type: 'number',
  },
  {
    label: 'Snapshot Number of Tables In Parallel',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        snapshotNumTablesInParallel: parseInt(value as string, 10) || 4,
      })),
    tips: 'Specify the number of tables to sync perform initial load for, in parallel. The default value is 4.',
    default: '4',
    type: 'number',
  },
  {
    label: 'Snapshot Staging Path',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        snapshotStagingPath: value as string | '',
      })),
    tips: 'You can specify staging path for Snapshot sync mode AVRO. For Snowflake as destination peer, this must be either empty or an S3 bucket URL. For BigQuery, this must be either empty or an existing GCS bucket name. In both cases, if empty, the local filesystem will be used.',
  },
  {
    label: 'CDC Staging Path',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        cdcStagingPath: (value as string) || '',
      })),
    tips: 'You can specify staging path for CDC sync mode AVRO. For Snowflake as destination peer, this must be either empty or an S3 bucket URL. For BigQuery, this must be either empty or an existing GCS bucket name. In both cases, if empty, the local filesystem will be used.',
  },
  {
    label: 'Soft Delete',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        softDelete: (value as boolean) || false,
      })),
    tips: 'Allows you to mark some records as deleted without actual erasure from the database',
    default: false,
    type: 'switch',
  },
  {
    label: 'Initial Copy Only',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        initialCopyOnly: (value as boolean) || false,
      })),
    tips: 'If set, PeerDB will only perform initial load and will not perform CDC sync.',
    type: 'switch',
    advanced: true,
  },
];
