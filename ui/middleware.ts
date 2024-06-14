import { Configuration } from '@/app/config/config';
import { withAuth } from 'next-auth/middleware';
import { NextRequest, NextResponse, userAgent } from 'next/server';

const authMiddleware = withAuth({});

// attempt to compare strings with constant time,
// nextjs edge runtime can't access crypto.timingSafeEqual
function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let ret = 0;
  for (let i = 0; i < a.length; i++) {
    ret |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return ret === 0;
}

export default async function middleware(req: NextRequest, resp: NextResponse) {
  const agent = userAgent(req);
  const xForwardedFor = req.headers.get('x-forwarded-for');

  if (Configuration.authentication.PEERDB_PASSWORD) {
    const authheader = req.headers.get('authorization');
    if (authheader && /^basic /i.test(authheader)) {
      const auth = atob(authheader.slice(6));
      if (
        auth[0] !== ':' ||
        !safeEqual(auth.slice(1), Configuration.authentication.PEERDB_PASSWORD)
      ) {
        return new NextResponse(null, { status: 403 });
      }
    } else {
      const authRes = await (authMiddleware as any)(req);
      if (authRes) return authRes;
    }
  }

  const res = NextResponse.next();
  console.log(
    `[${req.method} ${req.url}] [${req.ip} ${xForwardedFor}] (${JSON.stringify(agent.device)} ${JSON.stringify(agent.os)} ${JSON.stringify(agent.browser)}) ${res.status}`
  );
  return res;
}

export const config = {
  matcher: [
    // Match everything other than static assets
    '/((?!_next/static/|images/|favicon.ico$).*)',
  ],
};
