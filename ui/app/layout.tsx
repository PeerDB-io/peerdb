import type { Metadata } from 'next';
import { cookies } from 'next/headers';
import { AppThemeProvider, StyledComponentsRegistry } from '../lib/AppTheme';

export const metadata: Metadata = {
  title: 'Peerdb UI',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  // Read theme from cookie during SSR
  const cookieStore = cookies();
  const themeCookie = cookieStore.get('peerdb-theme');
  const initialTheme = themeCookie?.value === 'dark' ? 'dark' : 'light';
  return (
    <html lang='en' className={initialTheme}>
      <body>
        <StyledComponentsRegistry>
          <AppThemeProvider initialTheme={initialTheme}>{children}</AppThemeProvider>
        </StyledComponentsRegistry>
      </body>
    </html>
  );
}
