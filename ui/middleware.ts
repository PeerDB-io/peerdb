import { Configuration } from '@/app/config/config';
import { withAuth } from 'next-auth/middleware';
import { NextRequest, NextResponse, userAgent } from 'next/server';

const authMiddleware = withAuth({});

export default async function middleware(req: NextRequest, resp: NextResponse) {
  const accessLogUUID = crypto.randomUUID();
  const agent = userAgent(req);
  const xForwardedFor = req.headers.get('x-forwarded-for');
  console.log(
    `[${accessLogUUID}] [${req.method} ${req.url}] [${req.ip} ${xForwardedFor}] (${JSON.stringify(agent.device)} ${JSON.stringify(agent.os)} ${JSON.stringify(agent.browser)})`
  );

  if (Configuration.authentication.PEERDB_PASSWORD) {
    const authRes = await (authMiddleware as any)(req);

    const authResJson = NextResponse.next();
    console.log(
      `[${accessLogUUID}] [${req.method} ${req.url}] [${req.ip} ${xForwardedFor}] (${JSON.stringify(agent.device)} ${JSON.stringify(agent.os)} ${JSON.stringify(agent.browser)}) ${authResJson.status}`
    );

    return authRes;
  }

  const res = NextResponse.next();
  console.log(
    `[${accessLogUUID}] [${req.method} ${req.url}] [${req.ip} ${xForwardedFor}] (${JSON.stringify(agent.device)} ${JSON.stringify(agent.os)} ${JSON.stringify(agent.browser)}) ${res.status}`
  );
}

export const config = {
  matcher: [
    // Match everything other than static assets
    '/((?!_next/static/|images/|favicon.ico$).*)',
  ],
};
