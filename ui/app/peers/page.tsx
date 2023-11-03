import { DropDialog } from '@/components/DropDialog';
import { DBType, Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';
import { Suspense } from 'react';
import { Header } from '../../lib/Header';
import prisma from '../utils/prisma';
export const dynamic = 'force-dynamic';

function PeerRow({ peer }: { peer: Peer }) {
  const peerType = DBType[peer.type];
  return (
    <TableRow key={peer.name}>
      <TableCell variant='normal'>
        <Label
          as={Link}
          style={{ cursor: 'pointer' }}
          href={`/peers/${peer.name}`}
        >
          {peer.name}
        </Label>
      </TableCell>
      <TableCell>
        <Label>{peerType}</Label>
      </TableCell>
      <TableCell></TableCell>
      <TableCell>
        <DropDialog
          mode='PEER'
          dropArgs={{
            peerName: peer.name,
          }}
        />
      </TableCell>
    </TableRow>
  );
}

async function fetchPeers() {
  const peers = await prisma.peers.findMany({});
  return peers;
}

async function PeersTable({ title }: { title: string }) {
  let peers = await fetchPeers();
  return (
    <Table
      title={<Label variant='headline'>{title}</Label>}
      toolbar={{
        left: (
          <>
            <Button variant='normalBorderless'>
              <Icon name='chevron_left' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='chevron_right' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='refresh' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='help' />
            </Button>
            <Button variant='normalBorderless' disabled>
              <Icon name='download' />
            </Button>
          </>
        ),
        right: <SearchField placeholder='Search' />,
      }}
      header={
        <TableRow>
          <TableCell as='th'>
            <Label as='label' style={{ fontWeight: 'bold' }}>
              Peer Name
            </Label>
          </TableCell>
          <TableCell as='th'>
            <Label as='label' style={{ fontWeight: 'bold' }}>
              Peer Type
            </Label>
          </TableCell>
          <TableCell as='th'>
            <Label as='label' style={{ fontWeight: 'bold' }}>
              Status
            </Label>
          </TableCell>
          <TableCell as='th'></TableCell>
        </TableRow>
      }
    >
      {peers.map((peer) => (
        <PeerRow peer={peer} key={peer.name} />
      ))}
    </Table>
  );
}

function Loading() {
  return (
    <h2>
      <ProgressCircle variant='determinate_progress_circle' /> Loading...
    </h2>
  );
}

export default async function Peers() {
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
        <Suspense fallback={<Loading />}>
          <PeersTable title='All peers' />
        </Suspense>
      </Panel>
    </LayoutMain>
  );
}
