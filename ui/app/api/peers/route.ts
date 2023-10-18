import {
  PeerConfig,
  UCreatePeerResponse,
  UValidatePeerResponse,
} from '@/app/dto/PeersDTO';
import prisma from '@/app/utils/prisma';
import {
  BigqueryConfig,
  DBType,
  EventHubConfig,
  EventHubGroupConfig,
  Peer,
  PostgresConfig,
  S3Config,
  SnowflakeConfig,
  SqlServerConfig,
} from '@/grpc_generated/peers';
import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus,
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

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
    default:
      return;
  }
};

export async function POST(request: Request) {
  const body = await request.json();
  const { name, type, config, mode } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const peer = constructPeer(name, type, config);
  if (mode === 'validate') {
    const validateReq: ValidatePeerRequest = { peer };
    const validateStatus: ValidatePeerResponse = await fetch(
      `${flowServiceAddr}/v1/peers/validate`,
      {
        method: 'POST',
        body: JSON.stringify(validateReq),
      }
    ).then((res) => {
      return res.json();
    });
    let response: UValidatePeerResponse = {
      valid: validateStatus.status === ValidatePeerStatus.VALID,
      message: validateStatus.message,
    };
    return new Response(JSON.stringify(response));
  } else if (mode === 'create') {
    const req: CreatePeerRequest = { peer };
    const createStatus: CreatePeerResponse = await fetch(
      `${flowServiceAddr}/v1/peers/create`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });
    let response: UCreatePeerResponse = {
      created: createStatus.status === CreatePeerStatus.CREATED,
      message: createStatus.message,
    };
    return new Response(JSON.stringify(response));
  }
}

// GET all the peers from the database
export async function GET(request: Request) {
  const peers = await prisma.peers.findMany();
  const truePeers: Peer[] = peers.map((peer) => {
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
      | EventHubGroupConfig;
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
      default:
        return newPeer;
    }
    return newPeer;
  });
  return new Response(JSON.stringify(truePeers));
}
