'use client';

import { fetcher } from '@/app/utils/swr';
import useLocalStorage from '@/app/utils/useLocalStorage';
import Logout from '@/components/Logout';
import { PeerDBVersionResponse } from '@/grpc_generated/route';
import { BrandLogo } from '@/lib/BrandLogo';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithSelect } from '@/lib/Layout';
import { Sidebar, SidebarItem } from '@/lib/Sidebar';
import { SessionProvider } from 'next-auth/react';
import Link from 'next/link';
import useSWR from 'swr';

const centerFlexStyle = {
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  width: '100%',
  marginBottom: '0.5rem',
};

export default function SidebarComponent(props: { showLogout: boolean }) {
  const timezones = ['UTC', 'Local', 'Relative'];
  const [zone, setZone] = useLocalStorage('timezone-ui', '');

  const {
    data: version,
    isLoading,
  }: { data: PeerDBVersionResponse; error: any; isLoading: boolean } = useSWR(
    '/api/version',
    fetcher
  );

  const [sidebarState, setSidebarState] = useLocalStorage(
    'peerdb-sidebar',
    'open'
  );
  const sidebar = (
    <Sidebar
      style={{ width: sidebarState == 'closed' ? 'fit-content' : 'auto' }}
      topTitle={
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            width: '100%',
          }}
        >
          {sidebarState === 'open' && (
            <Label as={Link} href='/'>
              <div className='cursor-pointer'>
                <BrandLogo />
              </div>
            </Label>
          )}
          <Button
            variant='normalBorderless'
            aria-label='iconButton'
            onClick={() =>
              setSidebarState(sidebarState === 'open' ? 'closed' : 'open')
            }
          >
            <Icon
              name={
                sidebarState === 'closed' ? 'chevron_right' : 'chevron_left'
              }
            />
          </Button>
        </div>
      }
      bottomRow={
        sidebarState === 'open' ? (
          <>
            <div style={centerFlexStyle}>
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
                    {timezones.map((tz, id) => (
                      <option key={id} value={tz}>
                        {tz}
                      </option>
                    ))}
                  </select>
                }
              />
            </div>
            {props.showLogout && <Logout />}
          </>
        ) : (
          <></>
        )
      }
      bottomLabel={
        sidebarState === 'open' ? (
          <div style={centerFlexStyle}>
            <Label as='label' style={{ textAlign: 'center', fontSize: 15 }}>
              {' '}
              <b>Version: </b>
              {isLoading ? 'Loading...' : version?.version}
            </Label>
          </div>
        ) : (
          <></>
        )
      }
    >
      <SidebarItem
        as={Link}
        href={'/peers'}
        leadingIcon={<Icon name='cable' />}
      >
        {sidebarState === 'open' && 'Peers'}
      </SidebarItem>
      <SidebarItem
        as={Link}
        href={'/mirrors'}
        leadingIcon={<Icon name='compare_arrows' />}
      >
        {sidebarState === 'open' && 'Mirrors'}
      </SidebarItem>
      <SidebarItem
        as={Link}
        href={'/alert-config'}
        leadingIcon={<Icon name='notifications' />}
      >
        {sidebarState === 'open' && 'Alerts'}
      </SidebarItem>
      <SidebarItem
        as={Link}
        href={'/scripts'}
        leadingIcon={<Icon name='code' />}
      >
        {sidebarState === 'open' && 'Scripts'}
      </SidebarItem>
      <SidebarItem as={Link} href={'/certs'} leadingIcon={<Icon name='lock' />}>
        {sidebarState === 'open' && 'Certificates'}
      </SidebarItem>
      <SidebarItem
        as={Link}
        href={'/mirror-logs'}
        leadingIcon={<Icon name='receipt' />}
      >
        {sidebarState === 'open' && 'Logs'}
      </SidebarItem>
      <SidebarItem
        as={Link}
        href={'/settings'}
        leadingIcon={<Icon name='settings' />}
      >
        {sidebarState === 'open' && 'Settings'}
      </SidebarItem>
    </Sidebar>
  );
  return props.showLogout ? (
    <SessionProvider>{sidebar}</SessionProvider>
  ) : (
    sidebar
  );
}
