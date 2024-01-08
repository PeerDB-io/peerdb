import { NextRequest, NextResponse } from 'next/server';
import { withAuth } from 'next-auth/middleware';
import { Configuration } from '@/app/config/config';

const authMiddleware = withAuth({});


export default async function middleware(req: NextRequest, resp: NextResponse) {
  if (Configuration.authentication.PEERDB_PASSWORD) {
    return (authMiddleware as any)(req);
  }
}


export const config = {
  matcher: [
    // Match everything other than static assets
    '/((?!_next/static/|images/|favicon.ico$).*)',
  ],
};
