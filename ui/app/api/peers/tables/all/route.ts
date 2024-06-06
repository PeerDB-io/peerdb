import { UTablesAllResponse } from '@/app/dto/PeersDTO';
import { AllTablesResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const tableList: AllTablesResponse = await flowServiceClient
      .get<AllTablesResponse>(`/v1/peers/tables/all?peer_name=${peerName}`)
      .then((res) => res.data);
    let response: UTablesAllResponse = {
      tables: tableList.tables,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.error(message, e);
  }
}
