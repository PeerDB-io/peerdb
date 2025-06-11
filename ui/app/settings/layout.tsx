'use server';
import { Configuration } from '@/app/config/config';
import SidebarComponent from '@/components/SidebarComponent';
import { Layout } from '@/lib/Layout';
import { PropsWithChildren } from 'react';

export default async function PageLayout({ children }: PropsWithChildren) {
  return (
    <Layout
      sidebar={
        <SidebarComponent
          showLogout={!!Configuration.authentication.PEERDB_PASSWORD}
        />
      }
    >
      {children}
    </Layout>
  );
}
