import SidebarComponent from '@/components/SidebarComponent';
import { Header } from '@/lib/Header';
import { Layout, LayoutMain } from '@/lib/Layout';
import Password from '@/components/Password';
import {cookies} from 'next/headers'

export default function Home() {
  return (
    <Layout sidebar={<SidebarComponent />}>
      <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
        <Header variant='largeTitle'>PeerDB Home Page</Header>
        PEERDB_PASSWORD <Password value={cookies().get('password')?.value} />
      </LayoutMain>
    </Layout>
  );
}
