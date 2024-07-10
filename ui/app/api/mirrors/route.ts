import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export const dynamic = 'force-dynamic';

export async function GET(_request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/flows/list`);
}
