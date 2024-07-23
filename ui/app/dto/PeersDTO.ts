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
export type PeerSetter = React.Dispatch<React.SetStateAction<PeerConfig>>;
