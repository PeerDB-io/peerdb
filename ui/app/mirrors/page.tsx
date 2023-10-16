import { Badge } from '@/lib/Badge';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import Link from 'next/link';
import prisma from '../utils/prisma';

export const dynamic = 'force-dynamic';

const Badges = [
  <Badge variant='positive' key={1}>
    <Icon name='play_circle' />
    Active
  </Badge>,
  <Badge variant='warning' key={1}>
    <Icon name='pause_circle' />
    Paused
  </Badge>,
  <Badge variant='destructive' key={1}>
    <Icon name='dangerous' />
    Broken
  </Badge>,
  <Badge variant='normal' key={1}>
    <Icon name='pending' />
    Incomplete
  </Badge>,
];

async function CDCFlows() {
  const flows = await prisma.flows.findMany({
    include: {
      sourcePeer: true,
      destinationPeer: true,
    },
  });

  let cdcFlows = flows.filter((flow) => {
    return !flow.query_string;
  });

  return (
    <Table
      title={<Label variant='headline'>Change-data capture</Label>}
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
          <TableCell as='th'>Name</TableCell>
          <TableCell as='th'>Source</TableCell>
          <TableCell as='th'>Destination</TableCell>
          <TableCell as='th'>Start Time</TableCell>
          <TableCell as='th'>Status</TableCell>
        </TableRow>
      }
    >
      {cdcFlows.map((flow) => (
        <TableRow key={flow.id}>
          <TableCell variant='button'>
            <Checkbox />
          </TableCell>
          <TableCell>
            <Label as={Link} href={`/mirrors/edit/${flow.name}`}>
              <div className='cursor-pointer underline'>{flow.name}</div>
            </Label>
          </TableCell>
          <TableCell>
            <Label>{flow.sourcePeer.name}</Label>
          </TableCell>
          <TableCell>
            <Label>{flow.destinationPeer.name}</Label>
          </TableCell>
          <TableCell>
            <Label>
              {moment(flow.created_at).format('YYYY-MM-DD HH:mm:ss')}
            </Label>
          </TableCell>
          <TableCell>
            <Label>Status TBD</Label>
          </TableCell>
        </TableRow>
      ))}
    </Table>
  );
}

// query replication flows table like CDC flows table
async function QRepFlows() {
  const flows = await prisma.flows.findMany({
    include: {
      sourcePeer: true,
      destinationPeer: true,
    },
  });

  let qrepFlows = flows.filter((flow) => {
    return flow.query_string;
  });

  return (
    <Table
      title={<Label variant='headline'>Query replication</Label>}
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
          <TableCell as='th'>Name</TableCell>
          <TableCell as='th'>Source</TableCell>
          <TableCell as='th'>Destination</TableCell>
          <TableCell as='th'>Start Time</TableCell>
          <TableCell as='th'>Status</TableCell>
        </TableRow>
      }
    >
      {qrepFlows.map((flow) => (
        <TableRow key={flow.id}>
          <TableCell variant='button'>
            <Checkbox />
          </TableCell>
          <TableCell>
            <Label as={Link} href={`/mirrors/edit/${flow.name}`}>
              <div className='cursor-pointer underline'>{flow.name}</div>
            </Label>
          </TableCell>
          <TableCell>
            <Label>{flow.sourcePeer.name}</Label>
          </TableCell>
          <TableCell>
            <Label>{flow.destinationPeer.name}</Label>
          </TableCell>
          <TableCell>
            <Label>
              {moment(flow.created_at).format('YYYY-MM-DD HH:mm:ss')}
            </Label>
          </TableCell>
          <TableCell>
            <Label>Status TBD</Label>
          </TableCell>
        </TableRow>
      ))}
    </Table>
  );
}

export default async function Mirrors() {
  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <Button variant='normalSolid' as={Link} href={'/mirrors/create'}>
              New mirror
            </Button>
          }
        >
          Mirrors
        </Header>
      </Panel>
      <Panel>
        <CDCFlows />
      </Panel>
      <Panel className='mt-10'>
        <QRepFlows />
      </Panel>
    </LayoutMain>
  );
}
