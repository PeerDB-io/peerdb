'use client';
import { tableStyle } from '@/app/peers/[peerName]/style';
import { TableMapping } from '@/grpc_generated/flow';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell } from '@/lib/Table';
import { TableRow } from '@tremor/react';
import React, { useMemo, useState } from 'react';
import ColumnDisplayModal from './columnDisplayModal';

export default function TablePairs({
  tables,
  sourcePeerName,
}: {
  tables?: TableMapping[];
  sourcePeerName: string;
}) {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedTable, setSelectedTable] = useState<TableMapping | null>(null);

  const shownTables: TableMapping[] | undefined = useMemo(() => {
    const shownTables = tables?.filter(
      (table: TableMapping) =>
        table.sourceTableIdentifier.includes(searchQuery) ||
        table.destinationTableIdentifier.includes(searchQuery)
    );
    return shownTables?.length ? shownTables : tables;
  }, [tables, searchQuery]);

  const handleTableClick = (table: TableMapping) => {
    console.log('Clicked table:', table);
    console.log('Table columns:', table.columns);
    console.log('Columns length:', table.columns?.length);
    setSelectedTable(table);
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
    setSelectedTable(null);
  };

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
                onClick={() => handleTableClick(table)}
                style={{ cursor: 'pointer' }}
                className='hover:bg-gray-50 dark:hover:bg-gray-800'
              >
                <TableCell>{table.sourceTableIdentifier}</TableCell>
                <TableCell style={{ padding: '0.5rem' }}>
                  {table.destinationTableIdentifier}
                </TableCell>
              </TableRow>
            ))}
          </Table>
        </div>

        <ColumnDisplayModal
          isOpen={isModalOpen}
          onClose={() => setIsModalOpen(false)}
          sourceTableIdentifier={selectedTable?.sourceTableIdentifier ?? ''}
          destinationTableIdentifier={
            selectedTable?.destinationTableIdentifier ?? ''
          }
          tableMapping={selectedTable}
          sourcePeerName={sourcePeerName}
        />
      </div>
    )
  );
}
