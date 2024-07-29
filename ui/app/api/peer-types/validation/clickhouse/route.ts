import { ClickhouseHostRegex } from '@/app/dto/PeersDTO';
import { GetPeerDBClickhouseAllowedDomains } from '@/peerdb-env/allowed_ch_domains';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const chDomains = GetPeerDBClickhouseAllowedDomains();
  var chHostRegex: ClickhouseHostRegex = {
    allowedDomainsPattern: '.*',
    wrongDomainErrMsg: 'Invalid domain for Clickhouse host',
  };
  if (chDomains.length > 0) {
    chHostRegex = {
      allowedDomainsPattern: `(${chDomains.join('|')})$`,
      wrongDomainErrMsg:
        'Invalid domain for Clickhouse host. Allowed domains: ' +
        chDomains.join(', '),
    };
  }

  return new Response(JSON.stringify(chHostRegex));
}
