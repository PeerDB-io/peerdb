import { Configuration } from '@/app/config/config';
import { AuthOptions } from 'next-auth';
import CredentialsProvider from 'next-auth/providers/credentials';
import { Provider } from 'next-auth/providers/index';

function getEnabledProviders(): Provider[] {
  return [
    CredentialsProvider({
      name: 'Password',
      credentials: {
        password: { label: 'Password', type: 'password' },
      },
      async authorize(credentials, req) {
        if (
          credentials == null ||
          credentials.password != Configuration.authentication.PEERDB_PASSWORD
        ) {
          return null;
        }
        return { id: '1', name: 'Admin' };
      },
    }),
  ];
}
export const authOptions: AuthOptions = {
  providers: getEnabledProviders(),
  debug: false,
  session: {
    strategy: 'jwt',
    maxAge: 60 * 60, // 1h
  },
  // adapter: PrismaAdapter(prisma),
  secret: Configuration.authentication.NEXTAUTH_SECRET,
  theme: {
    colorScheme: 'light',
    logo: '/images/peerdb-combinedMark.svg',
  },
};
