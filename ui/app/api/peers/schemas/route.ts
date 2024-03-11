import { USchemasResponse } from '@/app/dto/PeersDTO';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName, peerType } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const schemaList = await fetch(
      `${flowServiceAddr}/v1/peers/schemas?peer_name=${peerName}&peer_type=${peerType}`
    ).then((res) => {
      return res.json();
    });
    let response: USchemasResponse = {
      schemas: schemaList.schemas,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
