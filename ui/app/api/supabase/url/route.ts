export const dynamic = 'force-dynamic';

export async function GET() {
  return Response.json({
    url: `https://api.supabase.com/v1/oauth/authorize?client_id=${encodeURIComponent(
      process.env.SUPABASE_ID ?? ''
    )}&response_type=code&redirect_uri=${encodeURIComponent(
      process.env.SUPABASE_REDIRECT ?? ''
    )}`,
  });
}
