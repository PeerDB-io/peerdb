'use server';
import { Configuration } from '@/app/config/config';
import SidebarComponent from '@/components/SidebarComponent';
import { Layout } from '@/lib/Layout';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { PropsWithChildren, Suspense } from 'react';

export default async function PageLayout({ children }: PropsWithChildren) {
  return (
    <Layout
      sidebar={
        <SidebarComponent
          showLogout={!!Configuration.authentication.PEERDB_PASSWORD}
        />
      }
    >
      <Suspense
        fallback={<ProgressCircle variant='determinate_progress_circle' />}
      >
        {children}
      </Suspense>
    </Layout>
  );
}
