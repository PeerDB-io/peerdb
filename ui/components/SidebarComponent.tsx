'use client';

import { BrandLogo } from '@/lib/BrandLogo';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Sidebar, SidebarItem } from '@/lib/Sidebar';
import Link from 'next/link';

export default function SidebarComponent() {
  return (
    <Sidebar
      topTitle={
        <Label as={Link} href='/'>
          <div className='cursor-pointer'>
            <BrandLogo />
          </div>
        </Label>
      }
      bottomRow={
        <>
          <Button className='w-full'>Help and Support</Button>
        </>
      }
      bottomLabel={<Label variant='footnote'>App. v0.7.0</Label>}
    >
      <SidebarItem
        as={Link}
        href={'/peers'}
        leadingIcon={<Icon name='cable' />}
      >
        Peers
      </SidebarItem>
      <SidebarItem
        as={Link}
        href={'/mirrors'}
        leadingIcon={<Icon name='compare_arrows' />}
      >
        Mirrors
      </SidebarItem>
    </Sidebar>
  );
}
