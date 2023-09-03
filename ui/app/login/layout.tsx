'use client';

import { Layout, LayoutMain } from '@/lib/Layout';
import { PropsWithChildren } from 'react';

export default function PageLayout({ children }: PropsWithChildren) {
  return (
    <Layout>
      <LayoutMain alignSelf='center' justifySelf='center' width='large'>
        {children}
      </LayoutMain>
    </Layout>
  );
}
