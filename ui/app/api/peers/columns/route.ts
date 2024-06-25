import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName, schemaName, tableName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(
    `${flowServiceAddr}/v1/peers/columns?peer_name=${encodeURIComponent(peerName)}&schema_name=${encodeURIComponent(schemaName)}&table_name=${encodeURIComponent(tableName)}`
  );
}
