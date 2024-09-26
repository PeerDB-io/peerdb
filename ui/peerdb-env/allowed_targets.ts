import 'server-only';

export function GetPeerDBClickHouseMode() {
  return process.env.PEERDB_ALLOWED_TARGETS === 'clickhouse';
}
