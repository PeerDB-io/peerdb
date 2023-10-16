import { PostgresConfig, SnowflakeConfig } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';

export type PeerConfig = PostgresConfig | SnowflakeConfig;
export type PeerSetter = Dispatch<SetStateAction<PeerConfig>>;
