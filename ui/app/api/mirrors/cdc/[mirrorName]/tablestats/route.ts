import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function GET(
  _: NextRequest,
  context: { params: { mirrorName: string } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(
    `${flowServiceAddr}/v1/mirrors/cdc/table_total_counts/${encodeURIComponent(
      context.params.mirrorName
    )}`,
    {
      cache: 'no-store',
    }
  );
}
