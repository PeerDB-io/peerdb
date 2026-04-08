'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { ColumnsItem } from '@/grpc_generated/route';
import { Checkbox } from '@/lib/Checkbox';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { Dispatch, SetStateAction } from 'react';

interface ColumnProps {
  columns: ColumnsItem[];
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

  return columns.map((column) => {
    const partOfOrderingKey = rows
      .find((row) => row.source == tableRow.source)
      ?.columns.some(
        (col) =>
          col.sourceName === column.name &&
          (col.ordering > 0 || col.partitioning > 0)
      );
    const isIncluded = !tableRow.exclude.has(column.name);
    const includedColumnCount = columns.length - tableRow.exclude.size;
    const isLastIncludedColumn = includedColumnCount === 1 && isIncluded;

    // For replica identity columns, ensure at least one remains included
    const replicaIdentityColumns = columns.filter(
      (col) => col.isReplicaIdentity
    );
    const includedReplicaIdentityCount = replicaIdentityColumns.filter(
      (col) => !tableRow.exclude.has(col.name)
    ).length;
    const isLastIncludedReplicaIdentity =
      !tableRow.isReplicaIdentityFull &&
      column.isReplicaIdentity &&
      includedReplicaIdentityCount === 1 &&
      isIncluded;

    return (
      <RowWithCheckbox
        key={column.name}
        label={
          <Label
            as='label'
            style={{
              fontSize: 13,
              display: 'flex',
              alignItems: 'center',
            }}
          >
            {column.name}
            <Label
              as='span'
              colorName='lowContrast'
              style={{
                marginLeft: '0.5rem',
                fontSize: 13,
              }}
            >
              {column.type}
            </Label>
          </Label>
        }
        action={
          <Checkbox
            style={{ cursor: 'pointer' }}
            disabled={
              isLastIncludedReplicaIdentity ||
              disabled ||
              partOfOrderingKey ||
              isLastIncludedColumn
            }
            checked={isIncluded}
            onCheckedChange={(state: boolean) =>
              handleColumnExclusion(column.name, state)
            }
          />
        }
      />
    );
  });
}
