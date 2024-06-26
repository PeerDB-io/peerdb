import { NextRequest } from 'next/server';

import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function GET(
  _request: NextRequest,
  context: { params: { peerName: string } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(
    `${flowServiceAddr}/v1/peers/info/${encodeURIComponent(context.params.peerName)}`
  );
}
