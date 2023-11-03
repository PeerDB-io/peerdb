'use client';
import { DropDialog } from '@/components/DropDialog';
import PeerButton from '@/components/PeerComponent';
import PeerTypeLabel from '@/components/PeerTypeComponent';
import { Peer } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useEffect, useState } from 'react';

function PeerRow({ peer }: { peer: Peer }) {
  return (
    <TableRow key={peer.name}>
      <TableCell variant='normal'>
        <PeerButton peerName={peer.name} peerType={peer.type} />
      </TableCell>
      <TableCell>
        <PeerTypeLabel ptype={peer.type} />
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

function PeersTable({ title, peers }: { title: string; peers: Peer[] }) {
  const [rows, setRows] = useState<Peer[]>([]);
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (searchQuery.length > 0) {
      setRows(
        peers.filter((peer) => {
          return peer.name.toLowerCase().includes(searchQuery.toLowerCase());
        })
      );
    }
    if (searchQuery.length == 0) {
      setRows(peers);
    }
  }, [searchQuery]);

  return (
    <Table
      title={<Label variant='headline'>{title}</Label>}
      toolbar={{
        left: <></>,
        right: (
          <SearchField
            placeholder='Search'
            value={searchQuery}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(e.target.value)
            }
          />
        ),
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
      {rows.map((row) => (
        <PeerRow peer={row} key={row.name} />
      ))}
    </Table>
  );
}

export default PeersTable;
