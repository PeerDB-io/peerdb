import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/mirrors/names`);
}
