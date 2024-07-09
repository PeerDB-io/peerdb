import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { name: string } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const timeSince = request.nextUrl.searchParams.get('timeSince');
  return fetch(
    `${flowServiceAddr}/v1/peers/slots/${encodeURIComponent(
      context.params.name
    )}/lag/${encodeURIComponent(timeSince ?? '')}`
  );
}
