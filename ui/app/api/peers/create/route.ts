import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus,
} from '@/grpc_generated/route';
import { GetFlowServiceClient } from '@/rpc/rpc';
import { constructPeer } from '../util';

export async function POST(request: Request) {
  const body = await request.json();
  const { name, type, config } = body;
  const flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
  const flowServiceClient = GetFlowServiceClient(flowServiceAddress);
  const peer = constructPeer(name, type, config);
  const req: CreatePeerRequest = {peer};
  const status: CreatePeerResponse = await flowServiceClient.createPeer(req);
  if (status.status === CreatePeerStatus.FAILED) {
    return new Response(status.message);
  } else if (status.status === CreatePeerStatus.CREATED) {
    return new Response('created');
  } else return new Response('status of peer creation is unknown');
}
