import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function GET() {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/dynamic_settings`, { cache: 'no-store' });
}

export async function POST(request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/dynamic_settings`, {
    method: 'POST',
    cache: 'no-store',
    body: request.body,
    duplex: 'half',
  } as any);
}
