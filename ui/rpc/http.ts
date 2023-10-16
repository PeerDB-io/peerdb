import 'server-only';

export function GetFlowHttpAddressFromEnv() {
  return process.env.PEERDB_FLOW_SERVER_HTTP!;
}
