import { USchemasResponse } from '@/app/dto/PeersDTO';
import { PeerSchemasResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
  
  export async function POST(request: Request) {
    const body = await request.json();
    const { peerName } = body;
    const flowServiceAddr = GetFlowHttpAddressFromEnv();
    const schemaList = await fetch(
    `${flowServiceAddr}/v1/peers/schemas?peer_name=${peerName}`,
    ).then((res) => {
      return res.json();
    });
    let response: USchemasResponse = {
      schemas: schemaList.schemas
    };
    if(schemaList.message === 'no rows in result set'){
      response = {
        schemas: []
      };
    }
    return new Response(JSON.stringify(response));
}