import { PeerConfig } from '@/app/peers/create/configuration/types';
import {
  DBType,
  Peer,
  PostgresConfig,
  SnowflakeConfig,
} from '@/grpc_generated/peers';
import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus,
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
} from '@/grpc_generated/route';
import { GetFlowServiceClientFromEnv } from '@/rpc/rpc';

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
  const flowServiceClient = GetFlowServiceClientFromEnv();
  const peer = constructPeer(name, type, config);
  if (mode === 'validate') {
    const validateReq: ValidatePeerRequest = { peer };
    const validateStatus: ValidatePeerResponse =
      await flowServiceClient.validatePeer(validateReq);
    if (validateStatus.status === ValidatePeerStatus.INVALID) {
      return new Response(validateStatus.message);
    } else if (validateStatus.status === ValidatePeerStatus.VALID) {
      return new Response('valid');
    }
  } else if (mode === 'create') {
    const req: CreatePeerRequest = { peer };
    const createStatus: CreatePeerResponse =
      await flowServiceClient.createPeer(req);
    if (createStatus.status === CreatePeerStatus.FAILED) {
      return new Response(createStatus.message);
    } else if (createStatus.status === CreatePeerStatus.CREATED) {
      return new Response('created');
    } else return new Response('status of peer creation is unknown');
  } else return new Response('mode of peer creation is unknown');
}
