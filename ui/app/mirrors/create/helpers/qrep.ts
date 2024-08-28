import {
  QRepConfig,
  QRepWriteMode,
  QRepWriteType,
  TypeSystem,
} from '@/grpc_generated/flow';

import { AdvancedSettingType, MirrorSetting } from './common';

export const qrepSettings: MirrorSetting[] = [
  {
    label: 'Source Table',
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
    label: 'Watermark Query',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        watermarkQuery: value as string,
      })),
    tips: 'Detection of new rows and logical partitioning will be based on this watermark query',
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
    label: 'Create Watermark Table On Destination',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        setupWatermarkTableOnDestination: value as boolean,
      })),
    checked: (state) => state.setupWatermarkTableOnDestination,
    tips: 'Specify if you want to create the watermark table on the destination as-is, can be used for some queries.',
    type: 'switch',
    default: false,
  },
  {
    label: 'Destination Table Name',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        destinationTableIdentifier: value as string,
      })),
    checked: (state) => state.destinationTableIdentifier,
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
    checked: (state) => state.initialCopyOnly,
    tips: 'Specify if you want query replication to stop at initial load.',
    type: 'switch',
  },
  {
    label: 'Sync Interval (Seconds)',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        waitBetweenBatchesSeconds: parseInt(value as string, 10) || 30,
      })),
    tips: 'Time to wait (in seconds) between getting partitions to process. The default is 30 seconds.',
    default: 30,
    type: 'number',
  },
  {
    label: 'Script',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({ ...curr, script: value as string })),
    tips: 'Script to use for row transformations. The default is no scripting.',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Use Postgres type system',
    stateHandler: (value, setter) =>
      setter((curr: QRepConfig) => ({
        ...curr,
        system: value === true ? TypeSystem.PG : TypeSystem.Q,
      })),
    checked: (state) => state.system === TypeSystem.PG,
    type: 'switch',
    default: false,
    tips: 'Decide if PeerDB should use native Postgres types directly',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Staging Path',
    stateHandler: (value, setter) =>
      setter(
        (curr: QRepConfig): QRepConfig => ({
          ...curr,
          stagingPath: value as string | '',
        })
      ),
    tips: 'For Snowflake as destination peer, this must be either empty or an S3 bucket URL. For BigQuery, this must be either empty or an existing GCS bucket name. In both cases, if empty, the local filesystem will be used.',
    advanced: AdvancedSettingType.ALL,
  },
];
