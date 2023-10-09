import { FlowConnectionConfigs, QRepSyncMode } from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { MirrorSetter } from '../../types';

export interface MirrorSetting {
  label: string;
  stateHandler: (
    value: string | Peer | boolean | QRepSyncMode,
    setter: MirrorSetter
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
  tableNameMapping: {},
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
};
