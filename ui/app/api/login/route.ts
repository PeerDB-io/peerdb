import { cookies } from 'next/headers';

export async function POST(request: Request) {
  const { password } = await request.json();
  if (process.env.PEERDB_PASSWORD !== password) {
    return new Response(
      JSON.stringify({
        error:
          'Your password is incorrect. Please check your password and try again.',
      })
    );
  }
  cookies().set('auth', password, {
    expires: Date.now() + 24 * 60 * 60 * 1000,
    secure: process.env.PEERDB_SECURE_COOKIES === 'true',
  });
  return new Response('{}');
}
