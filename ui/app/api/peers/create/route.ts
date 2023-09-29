import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus,
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
  let req: CreatePeerRequest = {
    peer: {
      name: name,
      type: typeMap(type),
      postgresConfig: config,
    },
  };
  let status: CreatePeerResponse = await flowServiceClient.createPeer(req);
  if (status.status === CreatePeerStatus.FAILED) {
    return new Response(status.message);
  } else if (status.status === CreatePeerStatus.CREATED) {
    return new Response('created');
  } else return new Response('status of peer creation is unknown');
}
