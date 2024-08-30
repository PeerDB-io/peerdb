import {
  ColumnSetting,
  FlowConnectionConfigs,
  TableEngine,
} from '@/grpc_generated/flow';

export enum MirrorType {
  CDC = 'CDC',
  QRep = 'Query Replication',
  XMin = 'XMin',
}

export type CDCConfig = FlowConnectionConfigs & {
  disablePeerDBColumns: boolean;
  envString: string;
};

export type TableMapRow = {
  schema: string;
  source: string;
  destination: string;
  partitionKey: string;
  exclude: Set<string>;
  selected: boolean;
  canMirror: boolean;
  tableSize: string;
  columnsToggleDisabled: boolean;
  engine: TableEngine;
  columns: ColumnSetting[];
};
