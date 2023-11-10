'use client';
import { DropDialog } from '@/components/DropDialog';
import PeerButton from '@/components/PeerComponent';
import SearchBar from '@/components/Search';
import TimeLabel from '@/components/TimeComponent';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';
import { useState } from 'react';

export function CDCFlows({ cdcFlows }: { cdcFlows: any }) {
  const [mirrors, setMirrors] = useState(cdcFlows);

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
              <SearchBar
                allItems={cdcFlows}
                setItems={setMirrors}
                filterFunction={(query: string) =>
                  cdcFlows.filter((flow: any) => {
                    return flow.name
                      .toLowerCase()
                      .includes(query.toLowerCase());
                  })
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
                <Label as={Link} href={`/mirrors/edit/${flow.name}`}>
                  <div className='cursor-pointer underline'>{flow.name}</div>
                </Label>
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

// query replication flows table like CDC flows table
export function QRepFlows({
  qrepFlows,
  title,
}: {
  qrepFlows: any;
  title: string;
}) {
  const [mirrors, setMirrors] = useState(qrepFlows);

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
              <SearchBar
                allItems={qrepFlows}
                setItems={setMirrors}
                filterFunction={(query: string) =>
                  qrepFlows.filter((flow: any) => {
                    return flow.name
                      .toLowerCase()
                      .includes(query.toLowerCase());
                  })
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
                <Label as={Link} href={`/mirrors/edit/${flow.name}`}>
                  <div className='cursor-pointer underline'>{flow.name}</div>
                </Label>
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
