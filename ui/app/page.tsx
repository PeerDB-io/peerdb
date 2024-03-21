import SidebarComponent from '@/components/SidebarComponent';
import { Header } from '@/lib/Header';
import { Layout, LayoutMain } from '@/lib/Layout';

export default function Home() {
  return (
    <Layout sidebar={<SidebarComponent />}>
      <LayoutMain alignSelf='center' justifySelf='center' width='full'>
        <Header variant='largeTitle'>PeerDB Home Page</Header>
      </LayoutMain>
    </Layout>
  );
}
