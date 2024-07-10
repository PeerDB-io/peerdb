import { FlowConnectionConfigs, QRepConfig } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';

export enum MirrorType {
  CDC = 'CDC',
  QRep = 'Query Replication',
  XMin = 'XMin',
}

export type UCreateMirrorResponse = {
  created: boolean;
};

export type UValidateMirrorResponse = {
  ok: boolean;
  errorMessage: string;
};

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

export type MirrorRowsData = {
  totalCount: number;
  insertsCount: number;
  updatesCount: number;
  deletesCount: number;
};

export type MirrorTableRowsData = {
  destinationTableName: string;
  data: MirrorRowsData;
};

export type UMirrorTableStatsResponse = {
  totalData: MirrorRowsData;
  tablesData: MirrorTableRowsData[];
};

export type MirrorLogsType = {
  flow_name: string;
  error_message: string;
  error_type: string;
  error_timestamp: Date;
}[];
