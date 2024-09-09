import { GetPeerDBEnableScripting } from '@/peerdb-env/experimental_enable_scripting';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const scriptingIsEnabled = GetPeerDBEnableScripting();
  return new Response(JSON.stringify(scriptingIsEnabled));
}
