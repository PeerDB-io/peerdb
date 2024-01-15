import {
  FlowConnectionConfigs,
  QRepSyncMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';

export interface MirrorSetting {
  label: string;
  stateHandler: (
    value: string | string[] | Peer | boolean | QRepSyncMode | QRepWriteType,
    setter: any
  ) => void;
  type?: string;
  required?: boolean;
  tips?: string;
  helpfulLink?: string;
  default?: string | number | boolean;
  advanced?: boolean; // whether it should come under an 'Advanced' section
}

export const blankCDCSetting: FlowConnectionConfigs = {
  source: undefined,
  destination: undefined,
  flowJobName: '',
  tableMappings: [],
  maxBatchSize: 1000000,
  doInitialSnapshot: true,
  publicationName: '',
  snapshotNumRowsPerPartition: 500000,
  snapshotMaxParallelWorkers: 1,
  snapshotNumTablesInParallel: 4,
  snapshotStagingPath: '',
  cdcStagingPath: '',
  softDelete: false,
  replicationSlotName: '',
  resync: false,
  softDeleteColName: '',
  syncedAtColName: '',
  initialSnapshotOnly: false,
  idleTimeoutSeconds: 60,
  forcePkeyChecks: false,
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
  setupWatermarkTableOnDestination: false,
  dstTableFullResync: false,
};
