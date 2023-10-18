import { QRepSyncMode } from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { MirrorSetting } from './common';
export const cdcSettings: MirrorSetting[] = [
  {
    label: 'Source Peer',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, source: value as Peer })),
    tips: 'The peer from which we will be replicating data. Ensure the prerequisites for this peer are met.',
    helpfulLink:
      'https://docs.peerdb.io/usecases/Real-time%20CDC/postgres-to-snowflake#prerequisites',
    type: 'select',
    required: true,
  },
  {
    label: 'Destination Peer',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, destination: value as Peer })),
    tips: 'The peer to which data will be replicated.',
    type: 'select',
    required: true,
  },
  {
    label: 'Initial Copy',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        doInitialCopy: (value as boolean) || false,
      })),
    tips: 'Specify if you want initial load to happen for your tables.',
    type: 'switch',
  },
  {
    label: 'Publication Name',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, publicationName: (value as string) || '' })),
    tips: 'If set, PeerDB will use this publication for the mirror.',
  },
  {
    label: 'Replication Slot Name',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        replicationSlotName: (value as string) || '',
      })),
    tips: 'If set, PeerDB will use this slot for the mirror.',
  },
  {
    label: 'Snapshot Number of Rows Per Partition',
    stateHandler: (value, setter) =>
      setter((curr) => ({
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
      setter((curr) => ({
        ...curr,
        snapshotMaxParallelWorkers: parseInt(value as string, 10) || 8,
      })),
    tips: 'PeerDB spins up parallel threads for each partition. This setting controls the number of partitions to sync in parallel. The default value is 8.',
    default: '8',
    type: 'number',
  },
  {
    label: 'Snapshot Number of Tables In Parallel',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        snapshotNumTablesInParallel: parseInt(value as string, 10) || 1,
      })),
    tips: 'Specify the number of tables to sync perform initial load for, in parallel. The default value is 1.',
    default: '1',
    type: 'number',
  },
  {
    label: 'Snapshot Sync Mode',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        snapshotSyncMode:
          (value as QRepSyncMode) || QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
      })),
    tips: 'Specify whether you want the sync mode for initial load to be via SQL or by staging AVRO files. The default mode is SQL.',
    default: 'SQL',
    type: 'select',
  },
  {
    label: 'CDC Sync Mode',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        cdcSyncMode:
          (value as QRepSyncMode) || QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
      })),
    tips: 'Specify whether you want the sync mode for CDC to be via SQL or by staging AVRO files. The default mode is SQL.',
    default: 'SQL',
    type: 'select',
  },
  {
    label: 'Snapshot Staging Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        snapshotStagingPath: value as string | '',
      })),
    tips: 'You can specify staging path if you have set the Snapshot sync mode as AVRO. For Snowflake as destination peer, this must be either empty or an S3 bucket URL.',
  },
  {
    label: 'CDC Staging Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, cdcStagingPath: (value as string) || '' })),
    tips: 'You can specify staging path if you have set the CDC sync mode as AVRO. For Snowflake as destination peer, this must be either empty or an S3 bucket url',
  },
  {
    label: 'Soft Delete',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, softDelete: (value as boolean) || false })),
    tips: 'Allows you to mark some records as deleted without actual erasure from the database',
    default: 'SQL',
    type: 'switch',
  },
];
