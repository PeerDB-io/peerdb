'use client';
import { tableStyle } from '@/app/peers/[peerName]/style';
import { TableMapping } from '@/grpc_generated/flow';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { displayQualifiedTable } from '@/lib/utils/tableIdentifier';
import React, { useMemo, useState } from 'react';
import { useTheme as useStyledTheme } from 'styled-components';
import ColumnDisplayModal from './columnDisplayModal';

function sourceDisplay(table: TableMapping): string {
  return table.sourceTable
    ? displayQualifiedTable(table.sourceTable)
    : table.sourceTableIdentifier;
}

function destinationDisplay(table: TableMapping): string {
  return table.destinationTable
    ? displayQualifiedTable(table.destinationTable)
    : table.destinationTableIdentifier;
}

export default function TablePairs({
  tables,
  sourcePeerName,
}: {
  tables?: TableMapping[];
  sourcePeerName: string;
}) {
  const styledTheme = useStyledTheme();
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedTable, setSelectedTable] = useState<TableMapping | null>(null);

  const shownTables: TableMapping[] | undefined = useMemo(() => {
    const shownTables = tables?.filter(
      (table: TableMapping) =>
        sourceDisplay(table).includes(searchQuery) ||
        destinationDisplay(table).includes(searchQuery)
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
        <div
          style={{
            ...tableStyle(styledTheme),
            maxHeight: '40vh',
            marginTop: '1rem',
          }}
        >
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
                key={`${sourceDisplay(table)}.${destinationDisplay(table)}`}
                onClick={() => handleTableClick(table)}
                style={{ cursor: 'pointer' }}
                className='hover:bg-gray-50 dark:hover:bg-gray-800'
              >
                <TableCell>{sourceDisplay(table)}</TableCell>
                <TableCell style={{ padding: '0.5rem' }}>
                  {destinationDisplay(table)}
                </TableCell>
              </TableRow>
            ))}
          </Table>
        </div>

        <ColumnDisplayModal
          isOpen={isModalOpen}
          onClose={() => setIsModalOpen(false)}
          sourceTableIdentifier={
            selectedTable ? sourceDisplay(selectedTable) : ''
          }
          destinationTableIdentifier={
            selectedTable ? destinationDisplay(selectedTable) : ''
          }
          tableMapping={selectedTable}
          sourcePeerName={sourcePeerName}
        />
      </div>
    )
  );
}
