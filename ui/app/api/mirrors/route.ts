import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export const dynamic = 'force-dynamic';

export async function GET(_: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return await fetch(`${flowServiceAddr}/v1/mirrors/list`, {
    cache: 'no-store',
  });
}
