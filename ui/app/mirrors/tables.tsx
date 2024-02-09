'use client';
import { DropDialog } from '@/components/DropDialog';
import MirrorLink from '@/components/MirrorLink';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';
import { useMemo, useState } from 'react';

export function CDCFlows({ cdcFlows }: { cdcFlows: any }) {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const mirrors = useMemo(
    () =>
      cdcFlows.filter((flow: any) => {
        return flow.name.toLowerCase().includes(searchQuery.toLowerCase());
      }),
    [searchQuery, cdcFlows]
  );

  return (
    <>
      <Label variant='headline'>Change-data capture</Label>
      <div
        style={{
          maxHeight: '35vh',
          overflow: 'scroll',
          width: '100%',
          marginTop: '1rem',
        }}
      >
        <Table
          toolbar={{
            left: <></>,
            right: (
              <SearchField
                placeholder='Search by flow name'
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  setSearchQuery(e.target.value)
                }
              />
            ),
          }}
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
          {mirrors.map((flow: any) => (
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
  const [searchQuery, setSearchQuery] = useState<string>('');
  const mirrors = useMemo(
    () =>
      qrepFlows.filter((flow: any) => {
        return flow.name.toLowerCase().includes(searchQuery.toLowerCase());
      }),
    [searchQuery, qrepFlows]
  );

  if (mirrors.length === 0) {
    return (
      <>
        <Label variant='headline'>{title}: None</Label>
      </>
    );
  }

  return (
    <>
      <Label variant='headline'>{title}</Label>
      <div
        style={{
          maxHeight: '35vh',
          overflow: 'scroll',
          width: '100%',
          marginTop: '1rem',
        }}
      >
        <Table
          toolbar={{
            left: <></>,
            right: (
              <SearchField
                placeholder='Search by flow name'
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  setSearchQuery(e.target.value)
                }
              />
            ),
          }}
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
          {mirrors.map((flow: any) => (
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
