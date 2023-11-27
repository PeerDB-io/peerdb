import SidebarComponent from '@/components/SidebarComponent';
import { Layout } from '@/lib/Layout';
import { cookies } from 'next/headers';
import { PropsWithChildren } from 'react';

export default function PageLayout({ children }: PropsWithChildren) {
  return (
    <Layout sidebar={<SidebarComponent logout={!!cookies().get('auth')} />}>
      {children}
    </Layout>
  );
}
