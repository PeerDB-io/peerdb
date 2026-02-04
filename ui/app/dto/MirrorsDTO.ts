import { FlowConnectionConfigs, TableMapping } from '@/grpc_generated/flow';

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
  'exclude' | 'sourceTableIdentifier' | 'destinationTableIdentifier'
> & {
  schema: string;
  source: string;
  destination: string;
  exclude: Set<string>;
  selected: boolean;
  canMirror: boolean;
  tableSize: string;
  editingDisabled: boolean;
  isReplicaIdentityFull: boolean;
};
