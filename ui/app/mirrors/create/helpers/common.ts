import { CDCConfig } from '@/app/dto/MirrorsDTO';
import { QRepConfig, TypeSystem } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';

export enum AdvancedSettingType {
  QUEUE = 'queue',
  ALL = 'all',
  NONE = 'none',
}

export interface MirrorSetting {
  label: string;
  stateHandler: (value: any, setter: any) => void;
  checked?: (setting: any) => boolean;
  type?: string;
  required?: boolean;
  tips?: string;
  helpfulLink?: string;
  default?: string | number | boolean;
  advanced?: AdvancedSettingType; // whether it should come under an 'Advanced' section
  command?: string;
}

export const SOFT_DELETE_COLUMN_NAME = '_PEERDB_IS_DELETED';

export const blankCDCSetting: CDCConfig = {
  sourceName: '',
  destinationName: '',
  flowJobName: '',
  tableMappings: [],
  maxBatchSize: 250000,
  doInitialSnapshot: true,
  publicationName: '',
  snapshotNumRowsPerPartition: 250000,
  snapshotNumPartitionsOverride: 0,
  snapshotMaxParallelWorkers: 4,
  snapshotNumTablesInParallel: 1,
  snapshotStagingPath: '',
  cdcStagingPath: '',
  replicationSlotName: '',
  resync: false,
  softDeleteColName: SOFT_DELETE_COLUMN_NAME,
  syncedAtColName: '_PEERDB_SYNCED_AT',
  initialSnapshotOnly: false,
  idleTimeoutSeconds: 60,
  script: '',
  system: TypeSystem.Q,
  disablePeerDBColumns: false,
  env: {},
  envString: '',
  version: 0,
  flags: [],
};

export const cdcSourceDefaults: { [index: string]: Partial<CDCConfig> } = {
  [DBType[DBType.BIGQUERY]]: {
    doInitialSnapshot: true,
    initialSnapshotOnly: true,
  },
};

export const blankQRepSetting: QRepConfig = {
  sourceName: '',
  destinationName: '',
  flowJobName: '',
  destinationTableIdentifier: '',
  query: '',
  watermarkTable: '',
  watermarkColumn: '',
  initialCopyOnly: false,
  maxParallelWorkers: 4,
  waitBetweenBatchesSeconds: 30,
  writeMode: undefined,
  stagingPath: '',
  numRowsPerPartition: 100000,
  numPartitionsOverride: 0,
  setupWatermarkTableOnDestination: false,
  dstTableFullResync: false,
  snapshotName: '',
  softDeleteColName: '_PEERDB_IS_DELETED',
  syncedAtColName: '_PEERDB_SYNCED_AT',
  script: '',
  system: TypeSystem.Q,
  env: {},
  version: 0,
  parentMirrorName: '',
  exclude: [],
  columns: [],
  sourceType: DBType.DBTYPE_UNKNOWN,
  flags: [],
};
