'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { Checkbox } from '@/lib/Checkbox';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { Dispatch, SetStateAction } from 'react';

interface ColumnProps {
  columns: string[];
  tableRow: TableMapRow;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}
export default function ColumnBox({
  columns,
  tableRow,
  rows,
  setRows,
}: ColumnProps) {
  const handleColumnExclusion = (
    source: string,
    column: string,
    include: boolean
  ) => {
    const currRows = [...rows];
    const rowOfSource = currRows.find((row) => row.source === source);
    if (rowOfSource) {
      if (include) {
        const updatedExclude = rowOfSource.exclude.filter(
          (col) => col !== column
        );
        rowOfSource.exclude = updatedExclude;
      } else {
        rowOfSource.exclude.push(column);
      }
    }
    setRows(currRows);
  };

  const columnExclusion = new Set(tableRow.exclude);
  return columns.map((column) => {
    const [columnName, columnType, isPkeyStr] = column.split(':');
    const isPkey = isPkeyStr === 'true';
    return (
      <RowWithCheckbox
        key={column}
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
            checked={!columnExclusion.has(columnName)}
            onCheckedChange={(state: boolean) =>
              handleColumnExclusion(tableRow.source, columnName, state)
            }
          />
        }
      />
    );
  });
}
