import {
  BigqueryConfig,
  ClickhouseConfig,
  ElasticsearchConfig,
  EventHubConfig,
  EventHubGroupConfig,
  KafkaConfig,
  PostgresConfig,
  PubSubConfig,
  S3Config,
  SnowflakeConfig,
} from '@/grpc_generated/peers';
import { TableResponse } from '@/grpc_generated/route';

export type UValidatePeerResponse = {
  valid: boolean;
  message: string;
};

export type UCreatePeerResponse = {
  created: boolean;
  message: string;
};

export type USchemasResponse = {
  schemas: string[];
};

export type UTablesResponse = {
  tables: TableResponse[];
};

export type UTablesAllResponse = {
  tables: string[];
};

export type UColumnsResponse = {
  columns: string[];
};

export type UDropPeerResponse = {
  dropped: boolean;
  errorMessage: string;
};

export type PeerConfig =
  | PostgresConfig
  | SnowflakeConfig
  | BigqueryConfig
  | ClickhouseConfig
  | S3Config
  | KafkaConfig
  | PubSubConfig
  | EventHubConfig
  | EventHubGroupConfig
  | ElasticsearchConfig;
export type CatalogPeer = {
  id: number;
  name: string;
  type: number;
  options: Buffer;
};
export type PeerSetter = React.Dispatch<React.SetStateAction<PeerConfig>>;

export type SlotLagPoint = {
  updatedAt: number;
  slotSize: number;
};

export type UPublicationsResponse = {
  publicationNames: string[];
};
