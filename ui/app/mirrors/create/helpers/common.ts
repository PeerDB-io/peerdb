import { FlowConnectionConfigs, TypeSystem } from '@/grpc_generated/flow';

export interface MirrorSetting {
  label: string;
  stateHandler: (value: any, setter: any) => void;
  type?: string;
  required?: boolean;
  tips?: string;
  helpfulLink?: string;
  default?: string | number | boolean;
  advanced?: boolean; // whether it should come under an 'Advanced' section
  command?: string;
}

export const blankCDCSetting: FlowConnectionConfigs = {
  source: undefined,
  destination: undefined,
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
  softDelete: true,
  replicationSlotName: '',
  resync: false,
  softDeleteColName: '',
  syncedAtColName: '',
  initialSnapshotOnly: false,
  idleTimeoutSeconds: 60,
  script: '',
  system: TypeSystem.Q,
};

export const blankQRepSetting = {
  destinationTableIdentifier: '',
  query: '',
  watermarkTable: '',
  watermarkColumn: '',
  initialCopyOnly: false,
  syncMode: 0,
  batchSizeInt: 0,
  batchDurationSeconds: 0,
  maxParallelWorkers: 4,
  waitBetweenBatchesSeconds: 30,
  writeMode: undefined,
  stagingPath: '',
  numRowsPerPartition: 100000,
  setupWatermarkTableOnDestination: true,
  dstTableFullResync: false,
};
