import 'server-only';

export function GetPeerDBClickHouseAllowedDomains() {
  const domains: string[] =
    process.env.PEERDB_CLICKHOUSE_ALLOWED_DOMAINS?.split(',')
      .map((domain) => domain.trim())
      .filter((domain) => domain.length > 0) || [];
  return domains;
}
