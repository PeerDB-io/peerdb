'use client';
import DropDialog from '@/components/DropDialog';
import MirrorLink from '@/components/MirrorLink';
import NewButton from '@/components/NewButton';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { ListMirrorsItem } from '@/grpc_generated/route';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';
import { MirrorType } from '../dto/MirrorsDTO';
import { tableStyle } from '../peers/[peerName]/style';

export function CDCFlows({ cdcFlows }: { cdcFlows: ListMirrorsItem[] }) {
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
          {cdcFlows.map((flow) => (
            <TableRow key={flow.id}>
              <TableCell>
                <MirrorLink flowName={flow.name} />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.sourceName}
                  peerType={flow.sourceType}
                />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.destinationName}
                  peerType={flow.destinationType}
                />
              </TableCell>
              <TableCell>
                <TimeLabel timeVal={new Date(flow.createdAt)} />
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
                    flowJobName: flow.name,
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
  qrepFlows: ListMirrorsItem[];
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
          targetPage={`/mirrors/create?type=${MirrorType.QRep}`}
          buttonText='Create your first query replication'
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
          {qrepFlows.map((flow) => (
            <TableRow key={flow.id}>
              <TableCell>
                <MirrorLink flowName={flow.name} />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.sourceName}
                  peerType={flow.sourceType}
                />
              </TableCell>
              <TableCell>
                <PeerButton
                  peerName={flow.destinationName}
                  peerType={flow.destinationType}
                />
              </TableCell>
              <TableCell>
                <TimeLabel timeVal={new Date(flow.createdAt)} />
              </TableCell>
              <TableCell>
                <DropDialog
                  mode='MIRROR'
                  dropArgs={{
                    flowJobName: flow.name,
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
