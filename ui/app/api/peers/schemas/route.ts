import { USchemasResponse } from '@/app/dto/PeersDTO';
import { PeerSchemasResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const schemaList = await flowServiceClient
      .get<PeerSchemasResponse>(`/v1/peers/schemas?peer_name=${peerName}`)
      .then((res) => res.data);
    let response: USchemasResponse = {
      schemas: schemaList.schemas,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
