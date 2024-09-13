import { NextRequest } from 'next/server';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  const params = await request.json();
  const supaResponse = await fetch('https://api.supabase.com/v1/oauth/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      Accept: 'application/json',
      Authorization: `Basic ${btoa(
        `${process.env.SUPABASE_ID}:${process.env.SUPABASE_SECRET}`
      )}`,
    },
    body: new URLSearchParams({
      grant_type: 'authorization_code',
      code: params.code,
      redirect_uri: process.env.SUPABASE_REDIRECT ?? '',
    }),
  });
  const supaResult = await supaResponse.json();
  const accessToken = supaResult.access_token;
  const projectsResponse = await fetch('https://api.supabase.com/v1/projects', {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  const projectsResult = await projectsResponse.json();
  return Response.json(
    projectsResult.map((x: any) => ({
      name: x.name,
      host: x.database.host,
    }))
  );
}
