'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import Link from 'next/link';
import { Header } from '../../lib/Header';
import PeersTable from './peersTable';
export const dynamic = 'force-dynamic';

import { ProgressCircle } from '@/lib/ProgressCircle';

import useSWR from 'swr';
import { fetcher } from '../utils/swr';

export default function Peers() {
  const { data: peers, error, isLoading } = useSWR('/api/peers', fetcher);

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <Button as={Link} href={'/peers/create'} variant='normalSolid'>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  whiteSpace: 'nowrap',
                }}
              >
                <Icon name='add' />
                <Label>New peer</Label>
              </div>
            </Button>
          }
        >
          Peers
        </Header>
      </Panel>
      <Panel>
        {isLoading && (
          <div className='h-screen flex items-center justify-center'>
            <ProgressCircle variant='determinate_progress_circle' />
          </div>
        )}
        {!isLoading && (
          <PeersTable
            title='All peers'
            peers={peers.map((peer: any) => peer)}
          />
        )}
      </Panel>
    </LayoutMain>
  );
}
