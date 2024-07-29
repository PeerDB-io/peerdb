import 'server-only';

export function GetPeerDBClickhouseAllowedDomains() {
  const domains: string[] =
    process.env.PEERDB_CLICKHOUSE_ALLOWED_DOMAINS?.split(',').map((domain) =>
      domain.trim()
    ) || [];
  return domains;
}
