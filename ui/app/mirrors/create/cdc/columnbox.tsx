'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { Checkbox } from '@/lib/Checkbox';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction } from 'react';

interface ColumnProps {
  columns: string[];
  tableRow: TableMapRow;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  disabled?: boolean;
  showOrdering: boolean;
}
export default function ColumnBox({
  columns,
  tableRow,
  rows,
  setRows,
  disabled,
  showOrdering,
}: ColumnProps) {
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

  return columns.map((column) => {
    const [columnName, columnType, isPkeyStr] = column.split(':');
    const isPkey = isPkeyStr === 'true';
    return (
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
            {showOrdering && !isPkey && (
              <TextField
                variant='simple'
                type='number'
                style={{ width: '3rem', marginLeft: '1rem', fontSize: 13 }}
                value={
                  tableRow.columns.find((col) => col.sourceName === columnName)
                    ?.ordering ?? 0
                }
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  handleColumnOrdering(columnName, +e.target.value)
                }
              />
            )}
          </Label>
        }
        action={
          <Checkbox
            style={{ cursor: 'pointer' }}
            disabled={isPkey || disabled}
            checked={!tableRow.exclude.has(columnName)}
            onCheckedChange={(state: boolean) =>
              handleColumnExclusion(columnName, state)
            }
          />
        }
      />
    );
  });
}
