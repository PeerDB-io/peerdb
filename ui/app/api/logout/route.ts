import { cookies } from 'next/headers';

export async function POST(req: Request) {
  cookies().delete('auth');
  return new Response('');
}
