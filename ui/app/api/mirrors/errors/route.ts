import {GetFlowHttpAddressFromEnv} from '@/rpc/http';

export async function POST(request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/mirrors/logs`, {
    method : 'POST',
    cache : 'no-cache',
    body : request.body,
    duplex : 'half',
  } as any);
}
