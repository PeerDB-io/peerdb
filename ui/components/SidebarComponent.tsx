'use client';

import { UVersionResponse } from '@/app/dto/VersionDTO';
import { fetcher } from '@/app/utils/swr';
import Logout from '@/components/Logout';
import { BrandLogo } from '@/lib/BrandLogo';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithSelect } from '@/lib/Layout';
import { Sidebar, SidebarItem } from '@/lib/Sidebar';
import Link from 'next/link';
import useSWR from 'swr';
import { useLocalStorage } from 'usehooks-ts';
import { SessionProvider } from 'next-auth/react';

const centerFlexStyle = {
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  width: '100%',
  marginBottom: '0.5rem',
};

export default function SidebarComponent(props: { }) {
  const timezones = ['UTC', 'Local', 'Relative'];
  const [zone, setZone] = useLocalStorage('timezone-ui', '');

  const {
    data: version,
    error,
    isLoading,
  }: { data: UVersionResponse; error: any; isLoading: boolean } = useSWR(
    '/api/version',
    fetcher
  );

  return (
    <SessionProvider>
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
          <div style={centerFlexStyle}>
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
                    value={zone}
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
          <Logout />
        </>
      }
      bottomLabel={
        <div style={centerFlexStyle}>
          <Label as='label' style={{ textAlign: 'center', fontSize: 15 }}>
            {' '}
            <b>Version: </b>
            {isLoading ? 'Loading...' : version?.version}
          </Label>
        </div>
      }
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
      <SidebarItem
        as={Link}
        href={'/alert-config'}
        leadingIcon={<Icon name='notifications' />}
      >
        Alert Configuration
      </SidebarItem>
    </Sidebar>
    </SessionProvider>
  );
}
