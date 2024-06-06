import { getTruePeer } from '@/app/api/peers/getTruePeer';
import {
  CatalogPeer,
  PeerConfig,
  UCreatePeerResponse,
  UValidatePeerResponse,
} from '@/app/dto/PeersDTO';
import prisma from '@/app/utils/prisma';
import {
  BigqueryConfig,
  ClickhouseConfig,
  DBType,
  ElasticsearchConfig,
  EventHubGroupConfig,
  KafkaConfig,
  Peer,
  PostgresConfig,
  PubSubConfig,
  S3Config,
  SnowflakeConfig,
} from '@/grpc_generated/peers';
import {
  CreatePeerRequest,
  CreatePeerStatus,
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
  createPeerStatusFromJSON,
  validatePeerStatusFromJSON,
} from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

const constructPeer = (
  name: string,
  type: string,
  config: PeerConfig
): Peer | undefined => {
  switch (type) {
    case 'POSTGRES':
      return {
        name,
        type: DBType.POSTGRES,
        postgresConfig: config as PostgresConfig,
      };
    case 'SNOWFLAKE':
      return {
        name,
        type: DBType.SNOWFLAKE,
        snowflakeConfig: config as SnowflakeConfig,
      };
    case 'BIGQUERY':
      return {
        name,
        type: DBType.BIGQUERY,
        bigqueryConfig: config as BigqueryConfig,
      };
    case 'CLICKHOUSE':
      return {
        name,
        type: DBType.CLICKHOUSE,
        clickhouseConfig: config as ClickhouseConfig,
      };
    case 'S3':
      return {
        name,
        type: DBType.S3,
        s3Config: config as S3Config,
      };
    case 'KAFKA':
      return {
        name,
        type: DBType.KAFKA,
        kafkaConfig: config as KafkaConfig,
      };
    case 'PUBSUB':
      return {
        name,
        type: DBType.PUBSUB,
        pubsubConfig: config as PubSubConfig,
      };
    case 'EVENTHUBS':
      return {
        name,
        type: DBType.EVENTHUBS,
        eventhubGroupConfig: config as EventHubGroupConfig,
      };
    case 'ELASTICSEARCH':
      return {
        name,
        type: DBType.ELASTICSEARCH,
        elasticsearchConfig: config as ElasticsearchConfig,
      };
    default:
      return;
  }
};

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  const body = await request.json();
  const { name, type, config, mode } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  const peer = constructPeer(name, type, config);
  if (mode === 'validate') {
    const validateReq: ValidatePeerRequest = { peer };
    try {
      const validateStatus: ValidatePeerResponse = await flowServiceClient
        .post(`/v1/peers/validate`, validateReq)
        .then((res) => {
          return res.data;
        });
      let response: UValidatePeerResponse = {
        valid:
          validatePeerStatusFromJSON(validateStatus.status) ===
          ValidatePeerStatus.VALID,
        message: validateStatus.message,
      };
      return new Response(JSON.stringify(response));
    } catch (error) {
      const message = await ParseFlowServiceErrorMessage(error);
      console.error('Error validating peer:', message, error);
    }
  } else if (mode === 'create') {
    const req: CreatePeerRequest = { peer };
    try {
      const createStatus = await flowServiceClient.post(
        `/v1/peers/create`,
        req
      );
      let response: UCreatePeerResponse = {
        created:
          createPeerStatusFromJSON(createStatus.status) ===
          CreatePeerStatus.CREATED,
        message: createStatus.message,
      };
      return new Response(JSON.stringify(response));
    } catch (error) {
      const message = await ParseFlowServiceErrorMessage(error);
      console.error('Error creating peer:', message, error);
    }
  }
}

// GET all the peers from the database
export async function GET(request: Request) {
  const peers = await prisma.peers.findMany();
  const truePeers: Peer[] = peers.map((peer: CatalogPeer) => getTruePeer(peer));
  return new Response(JSON.stringify(truePeers));
}
