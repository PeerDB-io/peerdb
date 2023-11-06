'use client';

import useTZStore from '@/app/globalstate/time';
import { BrandLogo } from '@/lib/BrandLogo';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithSelect } from '@/lib/Layout';
import { Sidebar, SidebarItem } from '@/lib/Sidebar';
import Link from 'next/link';

export default function SidebarComponent() {
  const timezones = ['UTC', 'Local', 'Relative'];
  const setZone = useTZStore((state) => state.setZone);
  const zone = useTZStore((state) => state.timezone);
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
          <div
            style={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              width: '100%',
              marginBottom: '0.5rem',
            }}
          >
            <div style={{ width: '80%' }}>
              <RowWithSelect
                label={<Label>Timezone:</Label>}
                action={
                  <select
                    style={{
                      borderRadius: '0.5rem',
                      padding: '0.2rem',
                      backgroundColor: 'transparent',
                      boxShadow: '0px 2px 4px rgba(0,0,0,0.1)',
                    }}
                    defaultValue={zone}
                    id='timeselect'
                    onChange={(e) => setZone(e.target.value)}
                  >
                    {timezones.map((tz, id) => {
                      return (
                        <option key={id} value={tz}>
                          {tz}
                        </option>
                      );
                    })}
                  </select>
                }
              />
            </div>
          </div>
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
