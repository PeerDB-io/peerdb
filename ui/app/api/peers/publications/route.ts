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
    const publicationList: PeerPublicationsResponse =
      await flowServiceClient.get(
        `/v1/peers/publications?peer_name=${peerName}`
      );
    let response: UPublicationsResponse = {
      publicationNames: publicationList.publicationNames,
    };
    console.log(response);
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
