import { UTablesResponse } from '@/app/dto/PeersDTO';
import { SchemaTablesResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const tableList: SchemaTablesResponse = await fetch(
    `${flowServiceAddr}/v1/peers/tables/all?peer_name=${peerName}`
  ).then((res) => {
    return res.json();
  });
  let response: UTablesResponse = {
    tables: tableList.tables,
  };
  return new Response(JSON.stringify(response));
}
