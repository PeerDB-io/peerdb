import { UTablesResponse } from '@/app/dto/PeersDTO';
import { SchemaTablesResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName, schemaName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const tableList: SchemaTablesResponse = await flowServiceClient
      .get<SchemaTablesResponse>(
        `/v1/peers/tables?peer_name=${peerName}&schema_name=${schemaName}`
      )
      .then((res) => res.data);
    let response: UTablesResponse = {
      tables: tableList.tables,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
