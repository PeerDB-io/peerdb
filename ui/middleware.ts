import type { NextRequest } from 'next/server';

import { NextResponse } from 'next/server';

export default function middleware(req: NextRequest) {
  if (
    req.nextUrl.pathname !== '/favicon.ico' &&
    req.nextUrl.pathname !== '/login' &&
    req.nextUrl.pathname !== '/api/login' &&
    !req.nextUrl.pathname.startsWith('/images/') &&
    !req.nextUrl.pathname.startsWith('/_next/static/') &&
    process.env.PEERDB_PASSWORD &&
    req.cookies.get('auth')?.value !== process.env.PEERDB_PASSWORD
  ) {
    req.cookies.delete('auth');
    return NextResponse.redirect(new URL('/login', req.url));
  }
  return NextResponse.next();
}
