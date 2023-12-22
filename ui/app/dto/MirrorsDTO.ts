import { FlowConnectionConfigs, QRepConfig } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';

export type UCreateMirrorResponse = {
  created: boolean;
};

export type UDropMirrorResponse = {
  dropped: boolean;
  errorMessage: string;
};

export type CDCConfig = FlowConnectionConfigs;
export type MirrorConfig = CDCConfig | QRepConfig;
export type MirrorSetter = Dispatch<SetStateAction<CDCConfig | QRepConfig>>;
export type TableMapRow = {
  schema: string;
  source: string;
  destination: string;
  partitionKey: string;
  exclude: string[];
  selected: boolean;
};

export type SyncStatusRow = {
  batchId: bigint;
  startTime: Date;
  endTime: Date | null;
  numRows: number;
};

export type AlertErr = {
  id: bigint;
  flow_name: string;
  error_message: string;
  error_type: string;
  error_timestamp: Date;
  ack: boolean;
};
