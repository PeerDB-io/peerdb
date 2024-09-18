'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Divider } from '@tremor/react';
import { Dispatch, SetStateAction, useState } from 'react';

interface ColumnProps {
  column: string;
  tableRow: TableMapRow;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  showColumnSettings: boolean;
}
export default function ColumnBox({
  column,
  tableRow,
  rows,
  setRows,
  showColumnSettings,
}: ColumnProps) {
  const [show, setShow] = useState(false);
  const handleColumnExclusion = (column: string, include: boolean) => {
    const source = tableRow.source;
    const currRows = [...rows];
    const rowIndex = currRows.findIndex((row) => row.source === source);
    if (rowIndex !== -1) {
      const sourceRow = currRows[rowIndex],
        newExclude = new Set(sourceRow.exclude);
      if (include) {
        newExclude.delete(column);
      } else {
        newExclude.add(column);
      }
      currRows[rowIndex] = {
        ...sourceRow,
        exclude: newExclude,
      };
      setRows(currRows);
    }
  };
  const handleColumnOrdering = (column: string, ordering: number) => {
    const source = tableRow.source;
    const currRows = [...rows];
    const rowIndex = currRows.findIndex((row) => row.source === source);
    if (rowIndex !== -1) {
      const sourceRow = currRows[rowIndex];
      const columns = [...sourceRow.columns];
      const colIndex = columns.findIndex((col) => col.sourceName === column);
      if (colIndex !== -1) {
        columns[colIndex] = { ...columns[colIndex], ordering };
      } else {
        columns.push({
          sourceName: column,
          destinationName: '',
          destinationType: '',
          ordering,
        });
      }
      currRows[rowIndex] = {
        ...sourceRow,
        columns,
      };
      setRows(currRows);
    }
  };

  const handleColumnTypeChange = (column: string, type: string) => {
    const source = tableRow.source;
    const currRows = [...rows];
    const rowIndex = currRows.findIndex((row) => row.source === source);
    if (rowIndex !== -1) {
      const sourceRow = currRows[rowIndex];
      const columns = [...sourceRow.columns];
      const colIndex = columns.findIndex((col) => col.sourceName === column);
      if (colIndex !== -1) {
        columns[colIndex] = {
          ...columns[colIndex],
          destinationType: type,
        };
      } else {
        columns.push({
          sourceName: column,
          destinationName: '',
          destinationType: type,
          ordering: 0,
        });
      }
      currRows[rowIndex] = {
        ...sourceRow,
        columns,
      };
      setRows(currRows);
    }
  };

  const [columnName, columnType, isPkeyStr] = column?.split(':');
  const isPkey = isPkeyStr === 'true';
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '0.5rem',
        marginBottom: '0.5rem',
        minWidth: '20%',
      }}
    >
      <div
        style={{
          display: 'flex',
          flexDirection: 'row',
          alignItems: 'center',
        }}
      >
        <RowWithCheckbox
          key={columnName}
          label={
            <Label
              as='label'
              style={{
                fontSize: 13,
                display: 'flex',
                alignItems: 'center',
              }}
            >
              {columnName}
              <p
                style={{
                  marginLeft: '0.5rem',
                  color: 'gray',
                }}
              >
                {columnType}
              </p>
            </Label>
          }
          action={
            <Checkbox
              style={{ cursor: 'pointer' }}
              disabled={isPkey}
              checked={!tableRow.exclude.has(columnName)}
              onCheckedChange={(state: boolean) =>
                handleColumnExclusion(columnName, state)
              }
            />
          }
        />
        {showColumnSettings && (
          <Button
            variant='peer'
            style={{ padding: 0 }}
            onClick={() => setShow((prev) => !prev)}
          >
            <Label as='label' style={{ fontSize: 12 }}>
              {' '}
              Configure{' '}
            </Label>
            <Icon name='arrow_drop_down' />
          </Button>
        )}
      </div>
      {show && showColumnSettings && (
        <div style={{ padding: '4px 8px' }}>
          {!isPkey && (
            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'center',
                columnGap: '1rem',
              }}
            >
              <div>
                <p style={{ fontSize: 13 }}>Ordering</p>
              </div>
              <TextField
                variant='simple'
                type='number'
                style={{ fontSize: 13, width: '3rem' }}
                value={
                  tableRow.columns.find((col) => col.sourceName === columnName)
                    ?.ordering ?? 0
                }
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  handleColumnOrdering(columnName, +e.target.value)
                }
              />
            </div>
          )}

          <div
            style={{
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              columnGap: '1rem',
            }}
          >
            <div>
              <p
                style={{
                  fontSize: 13,
                }}
              >
                Custom column type
              </p>
            </div>

            <TextField
              variant='simple'
              placeholder='Custom column type'
              style={{ width: '10rem', padding: '4px 8px', fontSize: 13 }}
              value={
                tableRow.columns.find((col) => col.sourceName === columnName)
                  ?.destinationType ?? ''
              }
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                handleColumnTypeChange(columnName, e.target.value)
              }
            />
          </div>
          <Divider style={{ marginTop: '0.5rem', marginBottom: '0.5rem' }} />
        </div>
      )}
    </div>
  );
}
