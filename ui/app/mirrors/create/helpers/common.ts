import {
  FlowConnectionConfigs,
  QRepWriteMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';

export interface MirrorSetting {
  label: string;
  stateHandler: (
    value: string | string[] | Peer | boolean | QRepWriteType,
    setter: any
  ) => void;
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
};

export const blankQRepSetting = {
  destinationTableIdentifier: '',
  query: `-- Here's a sample template:
  SELECT * FROM <table_name>
  WHERE <watermark_column>
  BETWEEN {{.start}} AND {{.end}}`,
  watermarkTable: '',
  watermarkColumn: '',
  initialCopyOnly: false,
  maxParallelWorkers: 4,
  waitBetweenBatchesSeconds: 30,
  writeMode: undefined,
  stagingPath: '',
  numRowsPerPartition: 100000,
  setupWatermarkTableOnDestination: false,
};

export const blankSnowflakeQRepSetting = {
  destinationTableIdentifier: '',
  query: '-- nothing',
  watermarkTable: '',
  watermarkColumn: '',
  maxParallelWorkers: 4,
  waitBetweenBatchesSeconds: 30,
  writeMode: {
    writeType: QRepWriteType.QREP_WRITE_MODE_OVERWRITE,
  } as QRepWriteMode,
  stagingPath: '',
  numRowsPerPartition: 100000,
  setupWatermarkTableOnDestination: true,
  initialCopyOnly: true,
};
