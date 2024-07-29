import 'server-only';

export function GetPeerDBClickhouseMode() {
  return process.env.PEERDB_ALLOWED_TARGETS === 'clickhouse';
}
