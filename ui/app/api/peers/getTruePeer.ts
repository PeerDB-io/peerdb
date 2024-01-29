import { CatalogPeer } from '@/app/dto/PeersDTO';
import {
  BigqueryConfig,
  ClickhouseConfig,
  EventHubConfig,
  EventHubGroupConfig,
  Peer,
  PostgresConfig,
  S3Config,
  SnowflakeConfig,
  SqlServerConfig,
} from '@/grpc_generated/peers';

export const getTruePeer = (peer: CatalogPeer) => {
  const newPeer: Peer = {
    name: peer.name,
    type: peer.type,
  };
  const options = peer.options;
  let config:
    | BigqueryConfig
    | SnowflakeConfig
    | PostgresConfig
    | EventHubConfig
    | S3Config
    | SqlServerConfig
    | EventHubGroupConfig
    | ClickhouseConfig;
  switch (peer.type) {
    case 0:
      config = BigqueryConfig.decode(options);
      newPeer.bigqueryConfig = config;
      break;
    case 1:
      config = SnowflakeConfig.decode(options);
      newPeer.snowflakeConfig = config;
      break;
    case 3:
      config = PostgresConfig.decode(options);
      newPeer.postgresConfig = config;
      break;
    case 4:
      config = EventHubConfig.decode(options);
      newPeer.eventhubConfig = config;
      break;
    case 5:
      config = S3Config.decode(options);
      newPeer.s3Config = config;
      break;
    case 6:
      config = SqlServerConfig.decode(options);
      newPeer.sqlserverConfig = config;
      break;
    case 7:
      config = EventHubGroupConfig.decode(options);
      newPeer.eventhubGroupConfig = config;
      break;
    case 8:
      config = ClickhouseConfig.decode(options);
      newPeer.clickhouseConfig = config;
      break;
    default:
      return newPeer;
  }
  return newPeer;
};
