import { DropDialog } from '@/components/DropDialog';
import { Button } from '@/lib/Button';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import Link from 'next/link';
import { getTruePeer } from '../api/peers/route';
import prisma from '../utils/prisma';

export const dynamic = 'force-dynamic';

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
    <>
      <Label variant='headline'>Change-data capture</Label>
      <div style={{ maxHeight: '35vh', overflow: 'scroll', width: '100%' }}>
        <Table
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
              <TableCell as='th'>Name</TableCell>
              <TableCell as='th'>Source</TableCell>
              <TableCell as='th'>Destination</TableCell>
              <TableCell as='th'>Start Time</TableCell>
              <TableCell as='th'></TableCell>
            </TableRow>
          }
        >
          {cdcFlows.map((flow) => (
            <TableRow key={flow.id}>
              <TableCell>
                <Label as={Link} href={`/mirrors/edit/${flow.name}`}>
                  <div className='cursor-pointer underline'>{flow.name}</div>
                </Label>
              </TableCell>
              <TableCell>
                <div style={{ cursor: 'pointer' }}>
                  <Label as={Link} href={`/peers/${flow.sourcePeer.name}`}>
                    {flow.sourcePeer.name}
                  </Label>
                </div>
              </TableCell>
              <TableCell>
                <div style={{ cursor: 'pointer' }}>
                  <Label as={Link} href={`/peers/${flow.destinationPeer.name}`}>
                    {flow.destinationPeer.name}
                  </Label>
                </div>
              </TableCell>
              <TableCell>
                <Label>
                  {moment(flow.created_at).format('YYYY-MM-DD HH:mm:ss')}
                </Label>
              </TableCell>
              <TableCell>
                <DropDialog
                  mode='MIRROR'
                  dropArgs={{
                    workflowId: flow.workflow_id,
                    flowJobName: flow.name,
                    sourcePeer: getTruePeer(flow.sourcePeer),
                    destinationPeer: getTruePeer(flow.destinationPeer),
                  }}
                />
              </TableCell>
            </TableRow>
          ))}
        </Table>
      </div>
    </>
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
    <>
      <Label variant='headline'>Query replication</Label>
      <div style={{ maxHeight: '35vh', overflow: 'scroll', width: '100%' }}>
        <Table
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
              <TableCell as='th'>Name</TableCell>
              <TableCell as='th'>Source</TableCell>
              <TableCell as='th'>Destination</TableCell>
              <TableCell as='th'>Start Time</TableCell>
              <TableCell as='th'></TableCell>
            </TableRow>
          }
        >
          {qrepFlows.map((flow) => (
            <TableRow key={flow.id}>
              <TableCell>
                <Label as={Link} href={`/mirrors/edit/${flow.name}`}>
                  <div className='cursor-pointer underline'>{flow.name}</div>
                </Label>
              </TableCell>
              <TableCell>
                <div style={{ cursor: 'pointer' }}>
                  <Label as={Link} href={`/peers/${flow.sourcePeer.name}`}>
                    {flow.sourcePeer.name}
                  </Label>
                </div>
              </TableCell>
              <TableCell>
                <div style={{ cursor: 'pointer' }}>
                  <Label as={Link} href={`/peers/${flow.destinationPeer.name}`}>
                    {flow.destinationPeer.name}
                  </Label>
                </div>
              </TableCell>
              <TableCell>
                <Label>
                  {moment(flow.created_at).format('YYYY-MM-DD HH:mm:ss')}
                </Label>
              </TableCell>
              <TableCell>
                <DropDialog
                  mode='MIRROR'
                  dropArgs={{
                    workflowId: flow.workflow_id,
                    flowJobName: flow.name,
                    sourcePeer: getTruePeer(flow.sourcePeer),
                    destinationPeer: getTruePeer(flow.destinationPeer),
                  }}
                />
              </TableCell>
            </TableRow>
          ))}
        </Table>
      </div>
    </>
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
