import {
  ValidatePeerRequest,
  ValidatePeerResponse,
  ValidatePeerStatus,
} from '@/grpc_generated/route';
import { GetFlowServiceClient } from '@/rpc/rpc';
import { constructPeer } from '../util';

export async function POST(request: Request) {
  const body = await request.json();
  const { name, type, config } = body;
  const flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
  const flowServiceClient = GetFlowServiceClient(flowServiceAddress);
  const peer = constructPeer(name, type, config);
  const req: ValidatePeerRequest = { peer };
  const status: ValidatePeerResponse =
    await flowServiceClient.validatePeer(req);
  if (status.status === ValidatePeerStatus.INVALID) {
    return new Response(status.message);
  } else if (status.status === ValidatePeerStatus.VALID) {
    return new Response('valid');
  } else return new Response('status of validate is unknown');
}
