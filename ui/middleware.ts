import type {NextRequest} from 'next/server';

import {NextResponse} from 'next/server'

export default function middleware(req: NextRequest) {
  if (req.nextUrl.pathname !== '/' &&
      !req.nextUrl.pathname.startsWith("/_next/static/") &&
      process.env.PEERDB_PASSWORD &&
      req.cookies.get('password')?.value !== process.env.PEERDB_PASSWORD) {
    return new Response('{}', {status : 401});
  }
  return NextResponse.next()
}
