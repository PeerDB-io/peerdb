import {NextResponse} from 'next/server'
import { cookies } from 'next/headers';

export async function POST(req: Request) {
  cookies().delete('auth');
  return NextResponse.redirect(new URL('/login', req.url))
}
