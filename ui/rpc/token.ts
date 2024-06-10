import 'server-only';

export function GetAuthorizationHeader() {
  const password = process.env.PEERDB_PASSWORD;
  if (!password) {
    return '';
  }
  return `Bearer ${Buffer.from(password).toString('base64')}`;
}
