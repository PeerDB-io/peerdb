import 'server-only';

export function GetPeerDBEnableScripting() {
  return process.env.PEERDB_EXPERIMENTAL_ENABLE_SCRIPTING === 'true';
}
