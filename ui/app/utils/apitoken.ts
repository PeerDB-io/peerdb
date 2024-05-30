import bcrypt from 'bcrypt';
import 'server-only';

function hashPassword(password: string, salt: number) {
  var hashed = bcrypt.hashSync(password, salt); // GOOD
  return hashed;
}

export function GetAPIToken() {
  const password = process.env.PEERDB_PASSWORD!;
  return hashPassword(password, 10);
}
