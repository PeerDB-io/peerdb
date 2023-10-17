import { DBType, Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Select } from '@/lib/Select';
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
      <TableCell variant='button'>
        <Checkbox />
      </TableCell>
      <TableCell variant='extended'>
        <Label as={Link} href='/peers/edit/TestPeer'>
          {peer.name}
        </Label>
      </TableCell>
      <TableCell>
        <Label>{peerType}</Label>
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
          <TableCell as='th' variant='button'>
            <Checkbox variant='mixed' defaultChecked />
          </TableCell>
          <TableCell as='th'>
            <Select placeholder='Select' />
          </TableCell>
          <TableCell as='th'>
            <Select placeholder='Select' />
          </TableCell>
          <TableCell as='th'>
            <Select placeholder='Select' />
          </TableCell>
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
