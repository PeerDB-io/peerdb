import { PostgresConfig, SnowflakeConfig } from '@/grpc_generated/peers';

export type UValidatePeerResponse = {
  valid: boolean;
  message: string;
};

export type UCreatePeerResponse = {
  created: boolean;
  message: string;
};

export type PeerConfig = PostgresConfig | SnowflakeConfig;
