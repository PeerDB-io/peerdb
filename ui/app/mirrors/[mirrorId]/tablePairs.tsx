'use client';
import { TableMapping } from '@/grpc_generated/flow';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import ColumnDisplayModal from './columnDisplayModal';

const ClickableTableRow = styled(TableRow)`
  cursor: pointer;

  &:hover {
    background-color: oklch(98.5% 0.002 247.839);
  }

  .dark &:hover {
    background-color: oklch(27.8% 0.033 256.848);
  }
`;

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
      <div>
        <div style={{ width: 'min(100%, 280px)', marginTop: '0.75rem' }}>
          <SearchField
            placeholder='Search by table name'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              setSearchQuery(e.target.value);
            }}
          />
        </div>
        <div
          style={{
            marginTop: '0.75rem',
          }}
        >
          <Table
            header={
              <TableRow>
                <TableCell>Source table</TableCell>
                <TableCell>Destination table</TableCell>
              </TableRow>
            }
          >
            {shownTables?.map((table) => (
              <ClickableTableRow
                key={`${table.sourceTableIdentifier}.${table.destinationTableIdentifier}`}
                onClick={() => handleTableClick(table)}
              >
                <TableCell>{table.sourceTableIdentifier}</TableCell>
                <TableCell style={{ padding: '0.5rem' }}>
                  {table.destinationTableIdentifier}
                </TableCell>
              </ClickableTableRow>
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
