'use client';
import { DropDialog } from '@/components/DropDialog';
import MirrorLink from '@/components/MirrorLink';
import NewButton from '@/components/NewButton';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';
import { MirrorType } from '../dto/MirrorsDTO';
import { tableStyle } from '../peers/[peerName]/style';

export function CDCFlows({ cdcFlows }: { cdcFlows: any }) {
  if (cdcFlows?.length === 0) {
    return (
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          rowGap: '1rem',
          width: 'fit-content',
        }}
      >
        <Label variant='headline'>Change-data capture</Label>
        <NewButton
          targetPage={`/mirrors/create?type=${MirrorType.CDC}`}
          buttonText={`Create your first CDC mirror`}
        />
      </div>
    );
  }

  return (
    <>
      <Label variant='headline'>Change-data capture</Label>
      <div style={{ ...tableStyle, maxHeight: '35vh' }}>
        <Table
          header={
            <TableRow>
              {['Name', 'Source', 'Destination', 'Start Time', 'Logs', ''].map(
                (heading, index) => (
                  <TableCell as='th' key={index}>
                    <Label
                      as='label'
                      style={{
                        fontWeight: 'bold',
                        padding: heading === 'Status' ? 0 : 'auto',
                      }}
                    >
                      {heading}
                    </Label>
                  </TableCell>
                )
              )}
            </TableRow>
          }
        >
          {cdcFlows.map((flow: any) => (
            <TableRow key={flow.id}>
              <TableCell>
                <MirrorLink flowName={flow?.name} />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.sourcePeer.name}
                  peerType={flow.sourcePeer.type}
                />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.destinationPeer.name}
                  peerType={flow.destinationPeer.type}
                />
              </TableCell>
              <TableCell>
                <TimeLabel timeVal={flow.created_at} />
              </TableCell>
              <TableCell>
                <Link href={`/mirrors/errors/${flow.name}`}>
                  <Icon name='description' />
                </Link>
              </TableCell>
              <TableCell>
                <DropDialog
                  mode='MIRROR'
                  dropArgs={{
                    workflowId: flow.workflow_id,
                    flowJobName: flow.name,
                    sourcePeer: flow.sourcePeer,
                    destinationPeer: flow.destinationPeer,
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
export function QRepFlows({
  qrepFlows,
  title,
}: {
  qrepFlows: any;
  title: string;
}) {
  if (qrepFlows?.length === 0) {
    return (
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          rowGap: '1rem',
          width: 'fit-content',
        }}
      >
        <Label variant='headline'>{title}</Label>
        <NewButton
          targetPage={`/mirrors/create?type=${title === 'XMIN Mirrors' ? MirrorType.XMin : MirrorType.QRep}`}
          buttonText={`Create your first ${title === 'XMIN Mirrors' ? 'XMIN mirror' : 'query replication'}`}
        />
      </div>
    );
  }

  return (
    <>
      <Label variant='headline'>{title}</Label>
      <div style={{ ...tableStyle, maxHeight: '35vh' }}>
        <Table
          header={
            <TableRow>
              {['Name', 'Source', 'Destination', 'Start Time', ''].map(
                (heading, index) => (
                  <TableCell as='th' key={index}>
                    <Label as='label' style={{ fontWeight: 'bold' }}>
                      {heading}
                    </Label>
                  </TableCell>
                )
              )}
            </TableRow>
          }
        >
          {qrepFlows.map((flow: any) => (
            <TableRow key={flow.id}>
              <TableCell>
                <MirrorLink flowName={flow?.name} />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.sourcePeer.name}
                  peerType={flow.sourcePeer.type}
                />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.destinationPeer.name}
                  peerType={flow.destinationPeer.type}
                />
              </TableCell>
              <TableCell>
                <TimeLabel timeVal={flow.created_at} />
              </TableCell>
              <TableCell>
                <DropDialog
                  mode='MIRROR'
                  dropArgs={{
                    workflowId: flow.workflow_id,
                    flowJobName: flow.name,
                    sourcePeer: flow.sourcePeer,
                    destinationPeer: flow.destinationPeer,
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
