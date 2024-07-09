import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function GET(_: NextRequest) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/scripts`, { cache: 'no-store' });
}

export async function POST(request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(`${flowServiceAddr}/v1/scripts`, {
    method: 'POST',
    cache: 'no-store',
    body: request.body,
    duplex: 'half',
  } as any);
}

export async function DELETE(request: NextRequest) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(
    `${flowServiceAddr}/v1/scripts/${Number(
      request.nextUrl.searchParams.get('id')
    )}`,
    {
      method: 'DELETE',
      cache: 'no-store',
    }
  );
}
