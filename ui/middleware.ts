import type {NextRequest} from 'next/server';

import {NextResponse} from 'next/server'

export default function middleware(request: NextRequest) {
  console.log(request);
  if (req.nextUrl.pathname !== '/' &&
      !req.nextUrl.pathname.startsWith("/_next/static/") &&
      process.env.PEERDB_PASSWORD &&
      req.cookies.get('password')?.value !== process.env.PEERDB_PASSWORD) {
    console.log('REJECT');
    return new Response('{}', {status : 401});
  }
  return NextResponse.next()
}

/*
export function middleware(req: NextRequest) {
  console.log(req);
  console.log(req.nextUrl);
}*/
