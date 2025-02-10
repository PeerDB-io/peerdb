'use server';
import { Configuration } from '@/app/config/config';
import SidebarComponent from '@/components/SidebarComponent';
import { Header } from '@/lib/Header';
import { Layout, LayoutMain } from '@/lib/Layout';

export default async function Home() {
  return (
    <Layout
      sidebar={
        <SidebarComponent
          showLogout={!!Configuration.authentication.PEERDB_PASSWORD}
        />
      }
    >
      <LayoutMain alignSelf='center' justifySelf='center' width='full'>
        <Header variant='largeTitle'>PeerDB Home Page</Header>
      </LayoutMain>
    </Layout>
  );
}
