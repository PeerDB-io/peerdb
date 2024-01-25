'use client';
import { TableMapping } from '@/grpc_generated/flow';
import { SearchField } from '@/lib/SearchField';
import { useMemo, useState } from 'react';

const TablePairs = ({ tables }: { tables?: TableMapping[] }) => {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const shownTables = useMemo(() => {
    const shownTables = tables?.filter(
      (table: TableMapping) =>
        table.sourceTableIdentifier.includes(searchQuery) ||
        table.destinationTableIdentifier.includes(searchQuery)
    );
    return shownTables?.length ? shownTables : tables;
  }, [tables, searchQuery]);
  if (tables)
    return (
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
            maxHeight: '80%',
            overflow: 'scroll',
            marginTop: '1rem',
          }}
        >
          <table
            style={{
              marginTop: '1rem',
              width: '100%',
              border: '1px solid #ddd',
              fontSize: 15,
              overflow: 'scroll',
            }}
          >
            <thead style={{ position: 'sticky', top: 0 }}>
              <tr
                style={{
                  borderBottom: '1px solid #ddd',
                  backgroundColor: '#f9f9f9',
                }}
              >
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.5rem',
                    fontWeight: 'bold',
                  }}
                >
                  Source Table
                </th>
                <th
                  style={{
                    textAlign: 'left',
                    padding: '0.5rem',
                    fontWeight: 'bold',
                  }}
                >
                  Destination Table
                </th>
              </tr>
            </thead>
            <tbody>
              {shownTables?.map((table) => (
                <tr
                  key={`${table.sourceTableIdentifier}.${table.destinationTableIdentifier}`}
                  style={{ borderBottom: '1px solid #ddd' }}
                >
                  <td style={{ padding: '0.5rem' }}>
                    {table.sourceTableIdentifier}
                  </td>
                  <td style={{ padding: '0.5rem' }}>
                    {table.destinationTableIdentifier}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
};

export default TablePairs;
