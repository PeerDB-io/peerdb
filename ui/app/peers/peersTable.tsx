'use client';
import DropDialog from '@/components/DropDialog';
import PeerButton from '@/components/PeerComponent';
import PeerTypeLabel, {
  DBTypeToGoodText,
} from '@/components/PeerTypeComponent';
import { DBType } from '@/grpc_generated/peers';
import { PeerListItem } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useMemo, useState } from 'react';
import ReactSelect from 'react-select';
import SelectTheme from '../styles/select';

function PeerRow({ peer }: { peer: PeerListItem }) {
  return (
    <TableRow key={peer.name}>
      <TableCell variant='normal'>
        <PeerButton peerName={peer.name} peerType={peer.type} />
      </TableCell>
      <TableCell>
        <PeerTypeLabel ptype={peer.type} />
      </TableCell>
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

export default function PeersTable({ peers }: { peers: PeerListItem[] }) {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [filteredType, setFilteredType] = useState<DBType | undefined>(
    undefined
  );
  const rows = useMemo(
    () =>
      peers.filter(
        (peer) =>
          peer.name.toLowerCase().includes(searchQuery.toLowerCase()) &&
          (filteredType == undefined || peer.type == filteredType)
      ),
    [peers, searchQuery, filteredType]
  );
  const allTypesOption: { value: DBType | undefined; label: string } = {
    value: undefined,
    label: 'All',
  };
  const availableTypes: { value: DBType | undefined; label: string }[] =
    Array.from(
      new Map( // Map filters out duplicates
        peers.map((peer) => [
          peer.type,
          { value: peer.type, label: DBTypeToGoodText(peer.type) },
        ])
      ).values()
    );
  availableTypes.unshift(allTypesOption);

  return (
    <Table
      toolbar={{
        left: <></>,
        right: (
          <>
            <ReactSelect
              className='w-48'
              options={availableTypes}
              onChange={(val, _) => {
                setFilteredType(val?.value);
              }}
              defaultValue={allTypesOption}
              theme={SelectTheme}
            />
            <SearchField
              placeholder='Search by peer name'
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setSearchQuery(e.target.value)
              }
            />
          </>
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
