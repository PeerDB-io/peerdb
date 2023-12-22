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
  default?: string | number;
  advanced?: boolean; // whether it should come under an 'Advanced' section
}

export const blankCDCSetting: FlowConnectionConfigs = {
  source: undefined,
  destination: undefined,
  flowJobName: '',
  tableSchema: undefined,
  tableMappings: [],
  srcTableIdNameMapping: {},
  tableNameSchemaMapping: {},
  metadataPeer: undefined,
  maxBatchSize: 100000,
  doInitialCopy: false,
  publicationName: '',
  snapshotNumRowsPerPartition: 500000,
  snapshotMaxParallelWorkers: 1,
  snapshotNumTablesInParallel: 1,
  snapshotSyncMode: 0,
  cdcSyncMode: 0,
  snapshotStagingPath: '',
  cdcStagingPath: '',
  softDelete: false,
  replicationSlotName: '',
  pushBatchSize: 0,
  pushParallelism: 0,
  resync: false,
  softDeleteColName: '',
  syncedAtColName: '',
  initialCopyOnly: false,
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
  maxParallelWorkers: 1,
  waitBetweenBatchesSeconds: 30,
  writeMode: undefined,
  stagingPath: '',
  numRowsPerPartition: 100000,
  setupWatermarkTableOnDestination: false,
  dstTableFullResync: false,
};
