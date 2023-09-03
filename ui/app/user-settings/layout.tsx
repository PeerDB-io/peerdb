'use client';

import { Avatar } from '@/lib/Avatar';
import { BrandLogo } from '@/lib/BrandLogo';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Layout, LayoutMain, Row } from '@/lib/Layout';
import { Select } from '@/lib/Select';
import { Sidebar, SidebarItem } from '@/lib/Sidebar';
import Link from 'next/link';
import { PropsWithChildren } from 'react';

export default function PageLayout({ children }: PropsWithChildren) {
  return (
    <Layout
      sidebar={() => (
        <Sidebar
          topTitle={() => (
            <Label>
              <BrandLogo />
            </Label>
          )}
          selectButton={() => <Select placeholder='Deployment name' />}
          avatar={() => (
            <Row
              thumbnail={() => <Avatar variant='text' text='JD' size='small' />}
              title={() => 'John Doe'}
            />
          )}
          bottomRow={() => (
            <>
              <Button className='w-full'>Help and Support</Button>
              <Button className='w-full'>Log out</Button>
            </>
          )}
          bottomLabel={() => <Label variant='footnote'>App. v0.7.0</Label>}
        >
          <SidebarItem
            leadingIcon={() => <Icon name='widgets' />}
            as={Link}
            href={'/dashboard'}
          >
            Dashboard
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/connectors'}
            leadingIcon={() => <Icon name='cable' />}
          >
            Connectors
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/mirrors'}
            leadingIcon={() => <Icon name='compare_arrows' />}
          >
            Mirrors
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/cloud'}
            leadingIcon={() => <Icon name='cloud' />}
          >
            Cloud
          </SidebarItem>
          <SidebarItem
            as={Link}
            href={'/user-settings'}
            selected
            leadingIcon={() => <Icon name='settings' fill />}
          >
            Settings
          </SidebarItem>
        </Sidebar>
      )}
    >
      <LayoutMain alignSelf='flex-start' justifySelf='center' width='xxLarge'>
        {children}
      </LayoutMain>
    </Layout>
  );
}
