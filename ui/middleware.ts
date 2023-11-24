import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';

export default function middleware(req: NextRequest) {
  if (
    req.nextUrl.pathname !== '/login' &&
    req.nextUrl.pathname !== '/api/login' &&
    req.nextUrl.pathname !== '/api/logout' &&
    process.env.PEERDB_PASSWORD &&
    req.cookies.get('auth')?.value !== process.env.PEERDB_PASSWORD
  ) {
    req.cookies.delete('auth');
    return NextResponse.redirect(new URL('/login?reject', req.url));
  }
  return NextResponse.next();
}

export const config = {
  matcher: [
    // Match everything other than static assets
    '/((?!_next/static/|images/|favicon.ico$).*)',
  ],
};
