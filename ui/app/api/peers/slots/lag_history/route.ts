import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function POST(request: NextRequest) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/peers/slots/lag_history`, {
    method: 'POST',
    cache: 'no-store',
    body: request.body,
    duplex: 'half',
  } as any);
}
