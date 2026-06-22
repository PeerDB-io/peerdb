import {
  FlowConnectionConfigs,
  QualifiedTable,
  TableMapping,
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

export type TableMapRow = Omit<
  TableMapping,
  | 'exclude'
  | 'sourceTableIdentifier'
  | 'destinationTableIdentifier'
  | 'sourceTable'
  | 'destinationTable'
> & {
  schema: string;
  // structured source table; `source` is its dotted display form, also used as row key
  sourceTable: QualifiedTable;
  source: string;
  // free-text destination input, parsed into a QualifiedTable on submit
  destination: string;
  exclude: Set<string>;
  selected: boolean;
  canMirror: boolean;
  tableSize: string;
  editingDisabled: boolean;
  isReplicaIdentityFull: boolean;
};
