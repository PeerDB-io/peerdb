import { NextRequest } from 'next/server';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  const supaResponse = await fetch('https://api.supabase.com/v1/oauth/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      Accept: 'application/json',
      Authorization: `Basic ${btoa(
        `${process.env.NEXT_PUBLIC_SUPABASE_ID}:${process.env.SUPABASE_SECRET}`
      )}`,
    },
    body: new URLSearchParams({
      grant_type: 'authorization_code',
      code: request.nextUrl.searchParams.get('code') || '',
      redirect_uri: process.env.NEXT_PUBLIC_SUPABASE_REDIRECT || '',
    }),
  });
  const supaResult = await supaResponse.json();
  const accessToken = supaResult.accessToken;
  const projectsResponse = await fetch('https://api.supabase.com/v1/projects', {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  const projectsResult = await projectsResponse.json();
  return projectsResult.map((x: any) => ({
    name: x.name,
    host: x.database.host,
  }));
}
