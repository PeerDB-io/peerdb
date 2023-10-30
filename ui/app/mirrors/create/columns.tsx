'use client';
import { Button } from '@/lib/Button';
import { Dispatch, SetStateAction, useState } from 'react';
import { fetchColumns } from './handlers';

interface ColumnsDisplayProps {
  setColumns: Dispatch<
    SetStateAction<
      {
        tableName: string;
        columns: string[];
      }[]
    >
  >;
  columns?: {
    tableName: string;
    columns: string[];
  }[];
  peerName: string;
  schemaName: string;
  tableName: string;
}

const ColumnsDisplay = (props: ColumnsDisplayProps) => {
  const [loading, setLoading] = useState(false);
  const addTableColumns = (table: string) => {
    // add table to columns
    fetchColumns(
      props.peerName,
      props.schemaName,
      props.tableName,
      setLoading
    ).then((res) =>
      props.setColumns((prev) => {
        return [...prev, { tableName: table, columns: res }];
      })
    );
  };

  const removeTableColumns = (table: string) => {
    // remove table from columns
    props.setColumns((prev) => {
      return prev.filter((column) => column.tableName !== table);
    });
  };

  const getTableColumns = (tableName: string) => {
    // get table columns
    return props.columns?.find((column) => column.tableName === tableName)
      ?.columns;
  };
  return (
    <div style={{ display: 'flex', flexDirection: 'column', width: '40%' }}>
      <Button
        onClick={() =>
          getTableColumns(props.tableName)
            ? removeTableColumns(props.tableName)
            : addTableColumns(props.tableName)
        }
        style={{
          height: '20%',
          backgroundColor: 'white',
          boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.2)',
          fontSize: 14,
        }}
      >
        {loading
          ? 'Loading...'
          : getTableColumns(props.tableName)
          ? 'Close'
          : 'Show columns'}
      </Button>

      <div
        style={{
          maxWidth: '100%',
          overflow: 'scroll',
          borderRadius: '0.5rem',
          boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.4)',
          backgroundColor: 'whitesmoke',
        }}
      >
        {getTableColumns(props.tableName)?.map((column, id) => {
          const columnName = column.split(':')[0];
          const columnType = column.split(':')[1];
          return (
            <div
              key={id}
              style={{
                fontSize: 15,
                fontFamily: 'monospace',
                width: '100%',
                paddingLeft: '0.3rem',
                paddingTop: '0.3rem',
                display: 'flex',
                background: 'ghostwhite',
                justifyContent: 'space-between',
              }}
            >
              <div
                style={{
                  width: '30%',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {columnName}
              </div>
              <div
                style={{
                  color: 'teal',
                  width: '40%',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {columnType}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default ColumnsDisplay;
