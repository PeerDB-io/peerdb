import SidebarComponent from '@/components/SidebarComponent';
import { Layout } from '@/lib/Layout';
import { PropsWithChildren } from 'react';

export default function PageLayout({ children }: PropsWithChildren) {
  return (
    <Layout sidebar={<SidebarComponent />}>
      {children}
    </Layout>
  );
}
