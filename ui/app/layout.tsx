import type { Metadata } from 'next';
import { AppThemeProvider, StyledComponentsRegistry } from '../lib/AppTheme';

export const metadata: Metadata = {
  title: 'Peerdb Cloud Template',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang='en'>
      <body>
        <StyledComponentsRegistry>
          <AppThemeProvider>{children}</AppThemeProvider>
        </StyledComponentsRegistry>
      </body>
    </html>
  );
}
