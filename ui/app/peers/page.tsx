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
  const peers: any[] = [];
  const { data, error, isLoading } = useSWR('/api/peers', fetcher);

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start'>
      <Panel style={{ width: '100%' }}>
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
        <Label>
          Peers represent a data store. Once you have a couple of peers, you can
          start moving data between them through mirrors.
        </Label>
      </Panel>
      <Panel>
        {isLoading && (
          <div className='h-screen flex items-center justify-center'>
            <ProgressCircle variant='determinate_progress_circle' />
          </div>
        )}
        {!isLoading &&
          (peers && peers.length == 0 ? (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                columnGap: '1rem',
              }}
            >
              <Button
                as={Link}
                href={'/peers/create'}
                style={{
                  width: 'fit-content',
                  boxShadow: '0px 2px 2px rgba(0,0,0,0.1)',
                }}
                variant='normalSolid'
              >
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    whiteSpace: 'nowrap',
                  }}
                >
                  <Icon name='add' />
                  <Label>Add your first peer</Label>
                </div>
              </Button>

              <Button
                as={Link}
                href={'https://docs.peerdb.io/features/supported-connectors'}
                target={'_blank'}
                style={{
                  width: 'fit-content',
                  boxShadow: '0px 2px 2px rgba(0,0,0,0.1)',
                }}
                variant='normal'
              >
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    whiteSpace: 'nowrap',
                  }}
                >
                  <Icon name='info' />
                  <Label>Learn more about peers</Label>
                </div>
              </Button>
            </div>
          ) : (
            <PeersTable peers={peers.map((peer: any) => peer)} />
          ))}
      </Panel>
    </LayoutMain>
  );
}
