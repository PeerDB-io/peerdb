import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import Link from 'next/link';
import { Header } from '../../lib/Header';
import { getTruePeer } from '../api/peers/route';
import prisma from '../utils/prisma';
import PeersTable from './peersTable';
export const dynamic = 'force-dynamic';

async function fetchPeers() {
  const peers = await prisma.peers.findMany({});
  return peers;
}

export default async function Peers() {
  let peers = await fetchPeers();
  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <Button
              as={Link}
              style={{
                width: '10%',
                height: '2rem',
                fontSize: 17,
                boxShadow: '0px 2px 4px rgba(0,0,0,0.2)',
              }}
              href={'/peers/create'}
              variant='normalSolid'
            >
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <Icon name='add' /> <Label>New peer</Label>
              </div>
            </Button>
          }
        >
          Peers
        </Header>
      </Panel>
      <Panel>
        <PeersTable
          title='All peers'
          peers={peers.map((peer) => getTruePeer(peer))}
        />
      </Panel>
    </LayoutMain>
  );
}
