import {
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
} from '@/grpc_generated/route';
import { GetFlowServiceClient } from '@/rpc/rpc';
import { typeMap } from '../peerTypeMap';

export async function POST(request: Request) {
  let body = await request.json();
  let name = body.name;
  let type = body.type;
  let config = body.config;
  let flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
  let flowServiceClient = GetFlowServiceClient(flowServiceAddress);
  let req: ValidatePeerRequest = {
    peer: {
      name: name,
      type: typeMap(type),
      postgresConfig: config,
    },
  };
  let status: ValidatePeerResponse = await flowServiceClient.validatePeer(req);
  if (status.status === ValidatePeerStatus.INVALID) {
    return new Response(status.message);
  } else if (status.status === ValidatePeerStatus.VALID) {
    return new Response('valid');
  } else return new Response('status of validate is unknown');
}
