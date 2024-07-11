import { USchemasResponse } from '@/app/dto/PeersDTO';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const schemaList = await fetch(
      `${flowServiceAddr}/v1/peers/schemas?peer_name=${peerName}`
    ).then((res) => res.json());
    let response: USchemasResponse = {
      schemas: schemaList.schemas,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
