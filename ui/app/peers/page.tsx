import { Button } from '@/lib/Button';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import Link from 'next/link';
import { Header } from '../../lib/Header';
import { getTruePeer } from '../api/peers/route';
import prisma from '../utils/prisma';
import PeersTable from './peerRows';
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
            <Button as={Link} href={'/peers/create'} variant='normalSolid'>
              New peer
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
