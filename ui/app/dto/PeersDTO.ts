import {
  BigqueryConfig,
  ClickhouseConfig,
  ElasticsearchConfig,
  EventHubConfig,
  EventHubGroupConfig,
  KafkaConfig,
  MySqlConfig,
  PostgresConfig,
  PubSubConfig,
  S3Config,
  SnowflakeConfig,
} from '@/grpc_generated/peers';

export type PeerConfig =
  | PostgresConfig
  | MySqlConfig
  | SnowflakeConfig
  | BigqueryConfig
  | ClickhouseConfig
  | S3Config
  | KafkaConfig
  | PubSubConfig
  | EventHubConfig
  | EventHubGroupConfig
  | ElasticsearchConfig;
export type PeerSetter = React.Dispatch<React.SetStateAction<PeerConfig>>;

export interface SupabaseListProjectsResponse {
  id: string;
  organization_id: string;
  name: string;
  region: string;
  created_at: string;
  database: {
    host: string;
    version: string;
  };
  status: 'ACTIVE_HEALTHY' | 'INACTIVE' | 'ERROR';
}
