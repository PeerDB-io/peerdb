import { Peer } from '@/grpc_generated/peers';
import { ListPeersRequest } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { SearchField } from '@/lib/SearchField';
import { Select } from '@/lib/Select';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { GetFlowServiceClient } from '@/rpc/rpc';
import Link from 'next/link';
import { Suspense } from 'react';
import { Header } from '../../lib/Header';

export const dynamic = 'force-dynamic';

async function fetchPeers() {
  let flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
  let flowServiceClient = GetFlowServiceClient(flowServiceAddress);
  let req: ListPeersRequest = {};
  let peers = await flowServiceClient.listPeers(req);
  return peers.peers;
}

function PeerRow({ peer }: { peer: Peer }) {
  return (
    <TableRow key={peer.name}>
      <TableCell variant='button'>
        <Checkbox />
      </TableCell>
      <TableCell variant='extended'>
        <Label as={Link} href='/connectors/edit/TestConnector'>
          {peer.name}
        </Label>
      </TableCell>
      <TableCell>
        <Label>{peer.type}</Label>
      </TableCell>
    </TableRow>
  );
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
  return <h2>🌀 Loading...</h2>;
}

export default async function Connectors() {
  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <Button as={Link} href={'/connectors/create'} variant='normalSolid'>
              New connector
            </Button>
          }
        >
          Connectors
        </Header>
      </Panel>
      <Panel>
        <Suspense fallback={<Loading />}>
          <PeersTable title='All connectors' />
        </Suspense>
      </Panel>
    </LayoutMain>
  );
}
