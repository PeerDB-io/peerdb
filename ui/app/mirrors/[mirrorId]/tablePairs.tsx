'use client';
import { tableStyle } from '@/app/peers/[peerName]/style';
import { TableMapping } from '@/grpc_generated/flow';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell } from '@/lib/Table';
import { TableRow } from '@tremor/react';
import { useMemo, useState } from 'react';

export default function TablePairs({ tables }: { tables?: TableMapping[] }) {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const shownTables: TableMapping[] | undefined = useMemo(() => {
    const shownTables = tables?.filter(
      (table: TableMapping) =>
        table.sourceTableIdentifier.includes(searchQuery) ||
        table.destinationTableIdentifier.includes(searchQuery)
    );
    return shownTables?.length ? shownTables : tables;
  }, [tables, searchQuery]);

  return (
    tables && (
      <div style={{ height: '30em' }}>
        <div style={{ width: '20%', marginTop: '2rem' }}>
          <SearchField
            placeholder='Search by table name'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              setSearchQuery(e.target.value);
            }}
          />
        </div>
        <div style={{ ...tableStyle, maxHeight: '40vh', marginTop: '1rem' }}>
          <Table
            header={
              <TableRow>
                <TableCell>Source Table</TableCell>
                <TableCell>Destination Table</TableCell>
              </TableRow>
            }
          >
            {shownTables?.map((table) => (
              <TableRow
                key={`${table.sourceTableIdentifier}.${table.destinationTableIdentifier}`}
              >
                <TableCell>{table.sourceTableIdentifier}</TableCell>
                <TableCell style={{ padding: '0.5rem' }}>
                  {table.destinationTableIdentifier}
                </TableCell>
              </TableRow>
            ))}
          </Table>
        </div>
      </div>
    )
  );
}
