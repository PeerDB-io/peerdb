import { UPublicationsResponse } from '@/app/dto/PeersDTO';
import { PeerPublicationsResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const publicationList: PeerPublicationsResponse = await flowServiceClient
      .get<PeerPublicationsResponse>(
        `/v1/peers/publications?peer_name=${peerName}`
      )
      .then((res) => res.data);
    let response: UPublicationsResponse = {
      publicationNames: publicationList.publicationNames,
    };
    console.log(response);
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
