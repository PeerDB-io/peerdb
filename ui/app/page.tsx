import { Avatar } from '@/lib/Avatar';
import { BrandLogo } from '@/lib/BrandLogo';
import { Button } from '@/lib/Button';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Layout, LayoutMain, Row } from '@/lib/Layout';
import { Select } from '@/lib/Select';
import { Sidebar, SidebarItem } from '@/lib/Sidebar';
import Link from 'next/link';

export default function Home() {
  return (
    <Layout
      sidebar={
        <Sidebar
          topTitle={
            <Label as={Link} href='/'>
              <BrandLogo />
            </Label>
          }
          selectButton={<Select placeholder='Deployment name' />}
          avatar={
            <Row
              thumbnail={<Avatar variant='text' text='JD' size='small' />}
              title={'John Doe'}
            />
          }
          bottomRow={
            <>
              <Button className='w-full'>Help and Support</Button>
              <Button className='w-full'>Log out</Button>
            </>
          }
          bottomLabel={<Label variant='footnote'>App. v0.7.0</Label>}
        >
          <SidebarItem
            leadingIcon={<Icon name='widgets' />}
            as={Link}
            href={'/dashboard'}
          >
            Dashboard
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/connectors'}
            leadingIcon={<Icon name='cable' />}
          >
            Connectors
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/mirrors'}
            leadingIcon={<Icon name='compare_arrows' />}
          >
            Mirrors
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/cloud'}
            leadingIcon={<Icon name='cloud' />}
          >
            Cloud
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/user-settings'}
            leadingIcon={<Icon name='settings' />}
          >
            Settings
          </SidebarItem>
        </Sidebar>
      }
    >
      <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
        <Header variant='largeTitle'>PeerDB Home Page</Header>
      </LayoutMain>
    </Layout>
  );
}
