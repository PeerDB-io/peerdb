import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return await fetch(
    `${flowServiceAddr}/v1/peers/schemas?peer_name=${peerName}`
  );
}
