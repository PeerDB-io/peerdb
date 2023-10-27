import {
  UCreatePeerResponse, USchemasResponse
} from '@/app/dto/PeersDTO';
import {
  CreatePeerRequest,
  CreatePeerResponse,
  CreatePeerStatus, PeerSchemasResponse, createPeerStatusFromJSON
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
  
  export async function GET(request: Request) {
    const body = await request.json();
    const { peerName, entity, schemaName, tableName } = body;
    const flowServiceAddr = GetFlowHttpAddressFromEnv();
    const schemaList: PeerSchemasResponse = await fetch(
      `${flowServiceAddr}/v1/peers/schemas?peer_name=${peerName}`,
    ).then((res) => {
      return res.json();
    });
    let response: USchemasResponse = {
      schemas: schemaList.schemas
    };
    return new Response(JSON.stringify(response));
    
}