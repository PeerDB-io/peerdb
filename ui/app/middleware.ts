import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'
 
export default async function middleware(req: NextRequest) {
  console.log(req.nextUrl);
  if (req.cookies.get('password') !== process.env.PEERDB_PASSWORD) {
    console.log('REJECT');
    return new Response('{}', { status: 401 });
  }
}