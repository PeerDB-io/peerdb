import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function POST(request: NextRequest) {
  const body = await request.json();

  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(
    `${flowServiceAddr}/v1/mirrors/${body.mirrorName}?` +
      new URLSearchParams({
        include_flow_info: 'true',
      }),
    {
      cache: 'no-store',
    }
  );
}
