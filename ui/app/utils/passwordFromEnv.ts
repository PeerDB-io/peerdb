import { createHash } from 'crypto';
import 'server-only';

export function GetHashedPeerDBPasswordFromEnv() {
  const password = process.env.PEERDB_PASSWORD!;
  const hash = createHash('sha256');
  hash.update(password);
  const hashedPassword = hash.digest('hex');
  return hashedPassword;
}
