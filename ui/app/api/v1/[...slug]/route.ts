import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function GET(
  request: NextRequest,
  { params }: { params: { slug: string[] } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  let url = `${flowServiceAddr}/v1/${params.slug
    .map((x) => encodeURIComponent(x))
    .join('/')}`;
  if (request.nextUrl.searchParams?.size) {
    url += `?${request.nextUrl.searchParams}`;
  }
  return fetch(url, {
    cache: 'no-store',
  });
}

export async function POST(
  request: NextRequest,
  { params }: { params: { slug: string[] } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  return fetch(
    `${flowServiceAddr}/v1/${params.slug
      .map((x) => encodeURIComponent(x))
      .join('/')}`,
    {
      method: 'POST',
      cache: 'no-store',
      body: request.body,
      duplex: 'half',
    } as any
  );
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: { slug: string[] } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  let url = `${flowServiceAddr}/v1/${params.slug
    .map((x) => encodeURIComponent(x))
    .join('/')}`;
  if (request.nextUrl.searchParams?.size) {
    url += `?${request.nextUrl.searchParams}`;
  }
  return fetch(url, {
    method: 'DELETE',
    cache: 'no-store',
  });
}
