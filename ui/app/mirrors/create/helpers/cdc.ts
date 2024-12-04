import { TypeSystem } from '@/grpc_generated/flow';
import { CDCConfig } from '../../../dto/MirrorsDTO';
import { AdvancedSettingType, blankCDCSetting, MirrorSetting } from './common';
export const cdcSettings: MirrorSetting[] = [
  {
    label: 'Initial Copy',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          doInitialSnapshot: (value as boolean) ?? true,
        })
      ),
    tips: 'Specify if you want initial load to happen for your tables.',
    type: 'switch',
    default: true,
    required: true,
  },
  {
    label: 'Pull Batch Size',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          maxBatchSize: (value as number) || 250000,
        })
      ),
    tips: 'The number of rows PeerDB will pull from source at a time. If left empty, the default value is 250,000 rows.',
    type: 'number',
    default: '250000',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Sync Interval (Seconds)',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          idleTimeoutSeconds: (value as number) || 60,
        })
      ),
    tips: 'Time after which a Sync flow ends, if it happens before pull batch size is reached. Defaults to 60 seconds.',
    helpfulLink: 'https://docs.peerdb.io/metrics/important_cdc_configs',
    type: 'number',
    default: '60',
    required: true,
    advanced: AdvancedSettingType.QUEUE,
  },
  {
    label: 'Publication Name',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          publicationName: (value as string) || '',
        })
      ),
    type: 'select',
    tips: 'PeerDB requires a publication associated with the tables you wish to sync.',
    helpfulLink:
      'https://www.postgresql.org/docs/current/sql-createpublication.html',
    command: 'CREATE PUBLICATION <publication_name> FOR ALL TABLES;',
  },
  {
    label: 'Replication Slot Name',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          replicationSlotName: (value as string) || '',
        })
      ),
    tips: 'If set, PeerDB will use this slot for the mirror.',
  },
  {
    label: 'Snapshot Number of Rows Per Partition',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          snapshotNumRowsPerPartition: parseInt(value as string, 10) || 250000,
        })
      ),
    tips: 'PeerDB splits up table data into partitions for increased performance. This setting controls the number of rows per partition. The default value is 250000.',
    default: '250000',
    type: 'number',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Parallelism for Initial Load',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        snapshotMaxParallelWorkers: parseInt(value as string, 10) || 4,
      })),
    tips: 'PeerDB spins up parallel threads for each partition in initial load. This setting controls the number of partitions to sync in parallel. The default value is 4.',
    default: '4',
    type: 'number',
    required: true,
  },
  {
    label: 'Snapshot Number of Tables In Parallel',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          snapshotNumTablesInParallel: parseInt(value as string, 10) || 1,
        })
      ),
    tips: 'Specify the number of tables to sync perform initial load for, in parallel. The default value is 1.',
    default: '1',
    type: 'number',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Snapshot Staging Path',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          snapshotStagingPath: value as string | '',
        })
      ),
    tips: 'You can specify staging path for Snapshot sync mode AVRO. For Snowflake as destination peer, this must be either empty or an S3 bucket URL. For BigQuery, this must be either empty or an existing GCS bucket name. In both cases, if empty, the local filesystem will be used.',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'CDC Staging Path',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          cdcStagingPath: (value as string) || '',
        })
      ),
    tips: 'You can specify staging path for CDC sync mode AVRO. For Snowflake as destination peer, this must be either empty or an S3 bucket URL. For BigQuery, this must be either empty or an existing GCS bucket name. In both cases, if empty, the local filesystem will be used.',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Soft Delete',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          softDeleteColName: (value as boolean)
            ? curr.softDeleteColName || blankCDCSetting.softDeleteColName
            : '',
        })
      ),
    tips: 'Allows you to mark some records as deleted without actual erasure from the database',
    default: true,
    type: 'switch',
    required: true,
  },
  {
    label: 'Initial Copy Only',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          initialSnapshotOnly: (value as boolean) ?? false,
        })
      ),
    tips: 'If set, PeerDB will only perform initial load and will not perform CDC sync.',
    type: 'switch',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Script',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          script: (value as string) || '',
        })
      ),
    tips: 'Associate PeerDB script with this mirror.',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Use Postgres type system',
    stateHandler: (value, setter) =>
      setter((curr: CDCConfig) => ({
        ...curr,
        system: value === true ? TypeSystem.PG : TypeSystem.Q,
      })),
    type: 'switch',
    default: false,
    tips: 'Decide if PeerDB should use native Postgres types directly',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Synced-At Column Name',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          syncedAtColName: value as string | '',
        })
      ),
    tips: 'A field to set the name of PeerDBs synced_at column. If not set, a default name will be set',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Soft Delete Column Name',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          softDeleteColName: value as string | '',
        })
      ),
    tips: 'A field to set the name of PeerDBs soft delete column.',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Disable all PeerDB columns (overrides any other setting)',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          disablePeerDBColumns: (value as boolean) ?? false,
        })
      ),
    type: 'switch',
    default: false,
    tips: 'Disables columns like synced_at, soft_delete etc. from being added to the destination table',
    advanced: AdvancedSettingType.ALL,
  },
  {
    label: 'Settings override',
    stateHandler: (value, setter) =>
      setter(
        (curr: CDCConfig): CDCConfig => ({
          ...curr,
          envString: value as string,
        })
      ),
    type: 'textarea',
    default: '',
    tips: '{"string":"string"} JSON mapping to override global settings for mirror',
    advanced: AdvancedSettingType.ALL,
  },
];
