import { FlowConnectionConfigs, QRepConfig } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';

export enum MirrorType {
  CDC = 'CDC',
  QRep = 'Query Replication',
  XMin = 'XMin',
}

export type CDCConfig = FlowConnectionConfigs & {
  disablePeerDBColumns: boolean;
};

export type MirrorConfig = CDCConfig | QRepConfig;
export type MirrorSetter = Dispatch<SetStateAction<CDCConfig | QRepConfig>>;
export type TableMapRow = {
  schema: string;
  source: string;
  destination: string;
  partitionKey: string;
  exclude: Set<string>;
  selected: boolean;
  canMirror: boolean;
  tableSize: string;
};
