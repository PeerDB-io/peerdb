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
