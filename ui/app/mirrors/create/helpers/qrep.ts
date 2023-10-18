import { QRepSyncMode } from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { MirrorSetting } from './common';
export const qrepSettings: MirrorSetting[] = [
  {
    label: 'Source Peer',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, sourcePeer: value as Peer })),
    tips: 'The peer from which we will be replicating data. Ensure the prerequisites for this peer are met.',
    helpfulLink:
      'https://docs.peerdb.io/usecases/Real-time%20CDC/postgres-to-snowflake#prerequisites',
    type: 'select',
    required: true,
  },
  {
    label: 'Destination Peer',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, destinationPeer: value as Peer })),
    tips: 'The peer to which data will be replicated.',
    type: 'select',
    required: true,
  },
  {
    label: 'Table',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, watermarkTable: (value as string) || '' })),
    tips: 'The source table of the replication and the table to which the watermark column belongs.',
    required: true,
  },
  {
    label: 'Watermark Column',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, watermarkColumn: (value as string) || '' })),
    tips: 'Watermark column is used to track the progress of the replication. This column should be a unique column in the query. Example: id',
    required: true,
  },
  {
    label: 'Create Destination Table',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        setupWatermarkTableOnDestination: (value as boolean) || false,
      })),
    tips: 'Specify if you want to create the watermark table on the destination as-is, can be used for some queries.',
    type: 'switch',
  },
  {
    label: 'Destination Table Name',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        destinationTableIdentifier: value as string,
      })),
    tips: 'Name of the destination. For any destination peer apart from BigQuery, this must be schema-qualified. Example: public.users',
    required: true,
  },
  {
    label: 'Rows Per Partition',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        numRowsPerPartition: parseInt(value as string, 10) || 500000,
      })),
    tips: 'PeerDB splits up table data into partitions for increased performance. This setting controls the number of rows per partition. The default value is 500000.',
    default: '500000',
    type: 'number',
  },
  {
    label: 'Maximum Parallel Workers',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        maxParallelWorkers: parseInt(value as string, 10) || 8,
      })),
    tips: 'PeerDB spins up parallel threads for each partition. This setting controls the number of partitions to sync in parallel. The default value is 8.',
    default: '8',
    type: 'number',
  },
  {
    label: 'Batch Size',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        batchSizeInt: parseInt(value as string, 10) || 1,
      })),
    tips: 'Size of each batch which is being synced.',
    default: '1',
    type: 'number',
  },
  {
    label: 'Batch Duration (Seconds)',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        snapshotNumTablesInParallel: parseInt(value as string, 10) || 3600,
      })),
    tips: 'Size of a batch as seconds when the watermark column is a timestamp column.',
    default: '3600',
    type: 'number',
  },
  {
    label: 'Sync Mode',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        syncMode:
          (value as QRepSyncMode) || QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
      })),
    tips: 'Specify whether you want the sync mode for initial load to be via SQL or by staging AVRO files. The default mode is SQL.',
    default: 'SQL',
    type: 'select',
  },
  {
    label: 'Staging Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, stagingPath: (value as string) || '' })),
    tips: `You can specify staging path if you have set the sync mode as AVRO. For Snowflake as destination peer.
    If this starts with gs:// then it will be written to GCS.
    If this starts with s3:// then it will be written to S3.
    If nothing is specified then it will be written to local disk.`,
  },
  {
    label: 'Initial Copy Only',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        initialCopyOnly: (value as boolean) || false,
      })),
    tips: 'Specify if you want query replication to stop at initial load.',
    type: 'switch',
  },
  {
    label: 'Wait Time Between Batches',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        waitBetweenBatchesSeconds: parseInt(value as string, 10) || 0,
      })),
    tips: 'Time to wait (in seconds) between getting partitions to process.',
    default: '0',
    type: 'number',
  },
];
