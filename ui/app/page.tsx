import Logout from '@/components/Logout';
import SidebarComponent from '@/components/SidebarComponent';
import { Header } from '@/lib/Header';
import { Layout, LayoutMain } from '@/lib/Layout';
import { cookies } from 'next/headers';

export default function Home() {
  return (
    <Layout sidebar={<SidebarComponent />}>
      <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
        <Header variant='largeTitle'>PeerDB Home Page</Header>
        {cookies().get('auth') && <Logout />}
      </LayoutMain>
    </Layout>
  );
}
