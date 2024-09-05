import { CDCConfig } from '@/app/dto/MirrorsDTO';
import { QRepConfig, TypeSystem } from '@/grpc_generated/flow';

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

export const blankCDCSetting: CDCConfig = {
  sourceName: '',
  destinationName: '',
  flowJobName: '',
  tableMappings: [],
  maxBatchSize: 1000000,
  doInitialSnapshot: true,
  publicationName: '',
  snapshotNumRowsPerPartition: 1000000,
  snapshotMaxParallelWorkers: 4,
  snapshotNumTablesInParallel: 1,
  snapshotStagingPath: '',
  cdcStagingPath: '',
  replicationSlotName: '',
  resync: false,
  softDeleteColName: '_PEERDB_IS_DELETED',
  syncedAtColName: '_PEERDB_SYNCED_AT',
  initialSnapshotOnly: false,
  idleTimeoutSeconds: 60,
  script: '',
  system: TypeSystem.Q,
  disablePeerDBColumns: false,
  env: {},
  envString: '',
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
  setupWatermarkTableOnDestination: false,
  dstTableFullResync: false,
  snapshotName: '',
  softDeleteColName: '_PEERDB_IS_DELETED',
  syncedAtColName: '',
  script: '',
  system: TypeSystem.Q,
  env: {},
  parentMirrorName: '',
};
