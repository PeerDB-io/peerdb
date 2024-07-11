import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/flows/cdc/create`, {
    method: 'POST',
    cache: 'no-store',
    body: request.body,
    duplex: 'half',
  } as any);
}
