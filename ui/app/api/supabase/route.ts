import { SupabaseListProjectsResponse } from '@/app/dto/PeersDTO';
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
  if (!supaResponse.ok) {
    const err = await supaResponse.text();
    return new Response(err, { status: supaResponse.status });
  }

  const supaResult = await supaResponse.json();
  const accessToken = supaResult.access_token;
  const projectsResponse = await fetch('https://api.supabase.com/v1/projects', {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!projectsResponse.ok) {
    const err = await projectsResponse.text();
    return new Response(err, { status: projectsResponse.status });
  }
  const projectsResult: SupabaseListProjectsResponse[] =
    await projectsResponse.json();
  return Response.json(projectsResult);
}
