import { UColumnsResponse } from '@/app/dto/PeersDTO';
import { TableColumnsResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName, schemaName, tableName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const columnsList: TableColumnsResponse =
      await flowServiceClient.get(
        `/v1/peers/columns?peer_name=${peerName}&schema_name=${schemaName}&table_name=${tableName}`
      );
    let response: UColumnsResponse = {
      columns: columnsList.columns,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
