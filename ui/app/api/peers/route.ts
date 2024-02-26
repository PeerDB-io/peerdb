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
  Peer,
  PostgresConfig,
  S3Config,
  SnowflakeConfig,
} from '@/grpc_generated/peers';
import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus,
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
  createPeerStatusFromJSON,
  validatePeerStatusFromJSON,
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
    default:
      return;
  }
};

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  const body = await request.json();
  const { name, type, config, mode } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const peer = constructPeer(name, type, config);
  if (mode === 'validate') {
    const validateReq: ValidatePeerRequest = { peer };
    try {
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
        valid:
          validatePeerStatusFromJSON(validateStatus.status) ===
          ValidatePeerStatus.VALID,
        message: validateStatus.message,
      };
      return new Response(JSON.stringify(response));
    } catch (error) {
      console.error('Error validating peer:', error);
    }
  } else if (mode === 'create') {
    const req: CreatePeerRequest = { peer };
    console.log('/peer/create req:', req);
    try {
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
        created:
          createPeerStatusFromJSON(createStatus.status) ===
          CreatePeerStatus.CREATED,
        message: createStatus.message,
      };
      return new Response(JSON.stringify(response));
    } catch (error) {
      console.error('Error creating peer:', error);
    }
  }
}

// GET all the peers from the database
export async function GET(request: Request) {
  const peers = await prisma.peers.findMany();
  const truePeers: Peer[] = peers.map((peer: CatalogPeer) => getTruePeer(peer));
  return new Response(JSON.stringify(truePeers));
}
