import { CatalogPeer } from '@/app/dto/PeersDTO';
import {
  BigqueryConfig,
  ClickhouseConfig,
  ElasticsearchConfig,
  EventHubGroupConfig,
  IcebergConfig,
  KafkaConfig,
  MySqlConfig,
  Peer,
  PostgresConfig,
  PubSubConfig,
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
  switch (peer.type) {
    case 0:
      newPeer.bigqueryConfig = BigqueryConfig.decode(options);
      break;
    case 1:
      newPeer.snowflakeConfig = SnowflakeConfig.decode(options);
      break;
    case 3:
      newPeer.postgresConfig = PostgresConfig.decode(options);
      break;
    case 5:
      newPeer.s3Config = S3Config.decode(options);
      break;
    case 6:
      newPeer.sqlserverConfig = SqlServerConfig.decode(options);
      break;
    case 7:
      newPeer.mysqlConfig = MySqlConfig.decode(options);
      break;
    case 8:
      newPeer.clickhouseConfig = ClickhouseConfig.decode(options);
      break;
    case 9:
      newPeer.kafkaConfig = KafkaConfig.decode(options);
      break;
    case 10:
      newPeer.pubsubConfig = PubSubConfig.decode(options);
      break;
    case 11:
      newPeer.eventhubGroupConfig = EventHubGroupConfig.decode(options);
      break;
    case 12:
      newPeer.elasticsearchConfig = ElasticsearchConfig.decode(options);
      break;
    case 13:
      newPeer.icebergConfig = IcebergConfig.decode(options);
      break;
    default:
      return newPeer;
  }
  return newPeer;
};
