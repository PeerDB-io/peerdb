import {
  QRepConfig,
  QRepWriteMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { MirrorSetting } from './common';
export const qrepSettings: MirrorSetting[] = [
  {
    label: 'Table',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        watermarkTable: (value as string) || '',
      })),
    type: 'select',
    tips: 'The source table of the replication and the table to which the watermark column belongs.',
    required: true,
  },
  {
    label: 'Watermark Column',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        watermarkColumn: (value as string) || '',
      })),
    type: 'select',
    tips: 'Watermark column is used to track the progress of the replication. This column should be a unique column in the query. Example: id',
    required: true,
  },
  {
    label: 'Create Destination Table',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        setupWatermarkTableOnDestination: (value as boolean) || false,
      })),
    tips: 'Specify if you want to create the watermark table on the destination as-is, can be used for some queries.',
    type: 'switch',
  },
  {
    label: 'Destination Table Name',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        destinationTableIdentifier: value as string,
      })),
    tips: 'Name of the destination. For any destination peer apart from BigQuery, this must be schema-qualified. Example: public.users',
    required: true,
  },
  {
    label: 'Rows Per Partition',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        numRowsPerPartition: parseInt(value as string, 10) || 100000,
      })),
    default: 100000,
    tips: 'PeerDB splits up table data into partitions for increased performance. This setting controls the number of rows per partition.',
    type: 'number',
    required: true,
  },
  {
    label: 'Maximum Parallel Workers',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        maxParallelWorkers: parseInt(value as string, 10) || 4,
      })),
    tips: 'PeerDB spins up parallel threads for each partition. This setting controls the number of partitions to sync in parallel. The default value is 4.',
    default: '4',
    type: 'number',
  },
  {
    label: 'Staging Path',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        stagingPath: (value as string) || '',
      })),
    tips: `You can specify staging path for sync mode AVRO. For Snowflake as destination peer:
    If this starts with s3:// then it will be written to S3.
    If nothing is specified then it will be written to local disk.`,
  },
  {
    label: 'Write Type',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => {
        let currWriteMode = curr.writeMode || { writeType: undefined };
        currWriteMode.writeType = value as QRepWriteType;
        return {
          ...curr,
          writeMode: currWriteMode,
        };
      }),
    tips: `Specify whether you want the write mode to be via APPEND, UPSERT or OVERWRITE.
    Append mode is for insert-only workloads. Upsert mode is append mode but also supports updates.
    Overwrite mode overwrites the destination table data every sync.`,
    type: 'select',
  },
  {
    label: 'Upsert Key Columns',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => {
        let defaultMode: QRepWriteMode = {
          writeType: QRepWriteType.QREP_WRITE_MODE_APPEND,
          upsertKeyColumns: [],
        };
        let currWriteMode = curr.writeMode || defaultMode;
        currWriteMode.upsertKeyColumns = value as string[];
        return {
          ...curr,
          writeMode: currWriteMode,
        };
      }),
    tips: `Needed when write mode is set to UPSERT.
    These columns need to be unique and are used for updates.`,
    type: 'select',
  },
  {
    label: 'Initial Copy Only',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        initialCopyOnly: (value as boolean) || false,
      })),
    tips: 'Specify if you want query replication to stop at initial load.',
    type: 'switch',
  },
  {
    label: 'Wait Time Between Batches',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        waitBetweenBatchesSeconds: parseInt(value as string, 10) || 30,
      })),
    tips: 'Time to wait (in seconds) between getting partitions to process. The default is 30 seconds',
    default: 30,
    type: 'number',
  },
];

export const snowflakeQRepSettings: MirrorSetting[] = [
  {
    label: 'Table',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        watermarkTable: (value as string) || '',
      })),
    type: 'text',
    tips: 'The source table of the replication and the table to which the watermark column belongs.',
    required: true,
  },
  {
    label: 'Create Destination Table',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        setupWatermarkTableOnDestination: (value as boolean) || false,
      })),
    tips: 'Specify if you want to create the watermark table on the destination as-is, can be used for some queries.',
    type: 'switch',
  },
  {
    label: 'Destination Table Name',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        destinationTableIdentifier: value as string,
      })),
    tips: 'Name of the destination. For any destination peer apart from BigQuery, this must be schema-qualified. Example: public.users',
    required: true,
  },
  {
    label: 'Write Type',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => {
        let currWriteMode = curr.writeMode || { writeType: undefined };
        currWriteMode.writeType = value as QRepWriteType;
        return {
          ...curr,
          writeMode: currWriteMode,
        };
      }),
    tips: `Specify whether you want the write mode to be via APPEND, UPSERT or OVERWRITE.
    Append mode is for insert-only workloads. Upsert mode is append mode but also supports updates.
    Overwrite mode overwrites the destination table data every sync.`,
    type: 'select',
  },
];
