import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const flowServiceAddr = GetFlowHttpAddressFromEnv();

  return fetch(`${flowServiceAddr}/v1/mirrors/state_change`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}
