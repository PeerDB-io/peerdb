'use client';
import SearchBar from '@/components/Search';
import { TableMapping } from '@/grpc_generated/flow';
import { useState } from 'react';

const TablePairs = ({ tables }: { tables?: TableMapping[] }) => {
  const [shownTables, setShownTables] = useState<TableMapping[] | undefined>(
    tables
  );
  if (tables)
    return (
      <>
        <div style={{ width: '20%', marginTop: '2rem' }}>
          <SearchBar
            allItems={tables}
            setItems={setShownTables}
            filterFunction={(query: string) =>
              tables.filter((table: TableMapping) => {
                return (
                  table.sourceTableIdentifier.includes(query) ||
                  table.destinationTableIdentifier.includes(query)
                );
              })
            }
          />
        </div>
        <table
          style={{
            marginTop: '1rem',
            borderCollapse: 'collapse',
            width: '100%',
            border: '1px solid #ddd',
            fontSize: 15,
          }}
        >
          <thead>
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
      </>
    );
};

export default TablePairs;
