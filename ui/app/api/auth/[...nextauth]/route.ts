import { getAuthOptions } from '@/app/auth/options';
import NextAuth from 'next-auth';
import { cookies } from 'next/headers';

async function getHandler() {
  const cookieStore = await cookies();
  const themeCookie = cookieStore.get('peerdb-theme');
  const colorScheme = themeCookie?.value === 'dark' ? 'dark' : 'light';
  return NextAuth(getAuthOptions(colorScheme));
}

export async function GET(
  request: Request,
  context: { params: Promise<{ nextauth: string[] }> }
) {
  const handler = await getHandler();
  return handler(request, context);
}

export async function POST(
  request: Request,
  context: { params: Promise<{ nextauth: string[] }> }
) {
  const handler = await getHandler();
  return handler(request, context);
}
