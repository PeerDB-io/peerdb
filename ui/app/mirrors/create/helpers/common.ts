import {
  FlowConnectionConfigs,
  QRepConfig,
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
  maxBatchSize: 0,
  doInitialCopy: false,
  publicationName: '',
  snapshotNumRowsPerPartition: 500000,
  snapshotMaxParallelWorkers: 8,
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
};

export const blankQRepSetting: QRepConfig = {
  flowJobName: '',
  sourcePeer: undefined,
  destinationPeer: undefined,
  destinationTableIdentifier: '',
  query: '',
  watermarkTable: '',
  watermarkColumn: '',
  initialCopyOnly: false,
  syncMode: 0,
  batchSizeInt: 0,
  batchDurationSeconds: 0,
  maxParallelWorkers: 8,
  waitBetweenBatchesSeconds: 30,
  writeMode: undefined,
  stagingPath: '',
  numRowsPerPartition: 0,
  setupWatermarkTableOnDestination: false,
  dstTableFullResync: false
};
