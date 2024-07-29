import { GetPeerDBClickhouseAllowedDomains } from '@/peerdb-env/allowed_ch_domains';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const chDomains = GetPeerDBClickhouseAllowedDomains();
  return new Response(JSON.stringify(chDomains));
}
