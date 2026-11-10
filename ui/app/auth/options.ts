import { Configuration } from '@/app/config/config';
import { scryptSync, timingSafeEqual } from 'crypto';
import { AuthOptions } from 'next-auth';
import CredentialsProvider from 'next-auth/providers/credentials';
import { Provider } from 'next-auth/providers/index';

// PEERDB_USERS="user:saltHex:scryptHashHex,user2:saltHex:hashHex"
function verifyUser(username: string, password: string): boolean {
  const entries = (process.env.PEERDB_USERS ?? '').split(',');
  for (const entry of entries) {
    const [user, salt, hash] = entry.trim().split(':');
    if (!user || !salt || !hash || user !== username) continue;
    const expected = Buffer.from(hash, 'hex');
    const actual = scryptSync(password, salt, expected.length);
    return timingSafeEqual(actual, expected);
  }
  return false;
}

function getEnabledProviders(): Provider[] {
  return [
    CredentialsProvider({
      name: 'PeerDB',
      credentials: {
        username: { label: 'Usuario', type: 'text' },
        password: { label: 'Contraseña', type: 'password' },
      },
      async authorize(credentials) {
        if (!credentials?.username || !credentials?.password) return null;
        if (!verifyUser(credentials.username, credentials.password)) return null;
        return { id: credentials.username, name: credentials.username };
      },
    }),
  ];
}

export function getAuthOptions(
  colorScheme: 'light' | 'dark' = 'light'
): AuthOptions {
  return {
    providers: getEnabledProviders(),
    debug: false,
    session: {
      strategy: 'jwt',
      maxAge: 24 * 7 * 60 * 60, // 1 week
    },
    secret: Configuration.authentication.NEXTAUTH_SECRET,
    theme: {
      colorScheme,
      logo:
        colorScheme === 'dark'
          ? '/images/peerdb-combinedMark-dark.svg'
          : '/images/peerdb-combinedMark.svg',
    },
  };
}
