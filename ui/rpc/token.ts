import bcrypt from 'bcrypt';
import 'server-only';

function hashPassword(password: string, rounds: number) {
  return bcrypt.hashSync(password, rounds);
}

export function GetAPIToken() {
  const password = process.env.PEERDB_PASSWORD!;
  const hashedPassword = hashPassword(password, 10);
  return Buffer.from(hashedPassword).toString('base64');
}
