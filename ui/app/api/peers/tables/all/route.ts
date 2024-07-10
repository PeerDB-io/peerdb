import { UTablesAllResponse } from '@/app/dto/PeersDTO';
import { AllTablesResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const tableList: AllTablesResponse = await fetch(
      `${flowServiceAddr}/v1/peers/tables/all?peer_name=${peerName}`
    ).then((res) => res.json());
    let response: UTablesAllResponse = {
      tables: tableList.tables,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
