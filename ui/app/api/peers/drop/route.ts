import { UDropPeerResponse } from '@/app/dto/PeersDTO';
import { DropPeerRequest, DropPeerResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  const req: DropPeerRequest = {
    peerName,
  };
  console.log('/drop/peer: req:', req);
  try {
    const dropStatus = await flowServiceClient
      .post<DropPeerResponse>(`/v1/peers/drop`, req)
      .then((res) => res.data);
    let response: UDropPeerResponse = {
      dropped: dropStatus.ok,
      errorMessage: dropStatus.errorMessage,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.error(message, e);
  }
}
