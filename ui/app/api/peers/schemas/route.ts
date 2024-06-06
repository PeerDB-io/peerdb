import { USchemasResponse } from '@/app/dto/PeersDTO';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const schemaList = await flowServiceClient.get(
      `/v1/peers/schemas?peer_name=${peerName}`
    );
    let response: USchemasResponse = {
      schemas: schemaList.schemas,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
