'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { ColumnSetting } from '@/grpc_generated/flow';
import { ColumnsItem } from '@/grpc_generated/route';
import { Checkbox } from '@/lib/Checkbox';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { Dispatch, Fragment, SetStateAction } from 'react';

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
  showOrdering,
}: ColumnProps) {
  // Helper to update a specific row
  const updateRow = (updater: (row: TableMapRow) => TableMapRow) => {
    const rowIndex = rows.findIndex((row) => row.source === tableRow.source);
    if (rowIndex !== -1) {
      const updatedRows = [...rows];
      updatedRows[rowIndex] = updater(updatedRows[rowIndex]);
      setRows(updatedRows);
    }
  };

  const handleColumnExclusion = (column: string, include: boolean) => {
    updateRow((row) => {
      const newExclude = new Set(row.exclude);
      if (include) {
        newExclude.delete(column);
      } else {
        newExclude.add(column);
      }
      return { ...row, exclude: newExclude };
    });
  };

  const handleNullableEnabledChange = (columnName: string, enabled: boolean) => {
    updateRow((row) => {
      const existingColumn = row.columns.find(
        (col) => col.sourceName === columnName
      );

      const updatedColumns: ColumnSetting[] = existingColumn
        ? // Update existing ColumnSetting
        row.columns.map((col) =>
          col.sourceName === columnName
            ? { ...col, nullableEnabled: enabled }
            : col
        )
        : // Create new ColumnSetting
        [
          ...row.columns,
          {
            sourceName: columnName,
            destinationName: '',
            destinationType: '',
            ordering: 0,
            partitioning: 0,
            nullableEnabled: enabled,
          },
        ];

      return { ...row, columns: updatedColumns };
    });
  };

  const getNullableEnabled = (columnName: string): boolean => {
    return (
      tableRow.columns.find((col) => col.sourceName === columnName)
        ?.nullableEnabled ?? false
    );
  };

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: showOrdering
          ? 'minmax(0, 2fr) minmax(0, 1fr) auto'
          : 'minmax(0, 2fr) minmax(0, 1fr)',
        alignItems: 'center',
        columnGap: '10rem',
        width: '80%',
      }}
    >
      <div style={{ fontSize: 12, fontWeight: 500 }}>Name</div>
      <div style={{ fontSize: 12, fontWeight: 500 }}>Type</div>
      {showOrdering &&
        <div style={{ fontSize: 12, fontWeight: 500 }}>Nullable</div>
      }

      {columns.map((column) => {
        const partOfOrderingKey = rows
          .find((row) => row.source == tableRow.source)
          ?.columns.some(
            (col) =>
              col.sourceName === column.name &&
              (col.ordering > 0 || col.partitioning > 0)
          );

        const isIncluded = !tableRow.exclude.has(column.name);
        const nullableEnabled = getNullableEnabled(column.name);

        return (
          <Fragment key={column.name}>
            <RowWithCheckbox
              label={
                <Label
                  as="label"
                  style={{
                    fontSize: 13,
                    display: 'flex',
                    alignItems: 'center',
                  }}
                >
                  {column.name}
                </Label>
              }
              action={
                <Checkbox
                  style={{ cursor: 'pointer' }}
                  disabled={column.isKey || disabled || partOfOrderingKey}
                  checked={isIncluded}
                  onCheckedChange={(state: boolean) =>
                    handleColumnExclusion(column.name, state)
                  }
                />
              }
            />

            <div
              style={{
                fontSize: 13,
                color: 'gray',
                whiteSpace: 'nowrap',
              }}
            >
              {column.type}
            </div>

            {showOrdering && (
              <Checkbox
                style={{
                  cursor: isIncluded && !disabled ? 'pointer' : 'default',
                  justifySelf: 'flex-start',
                }}
                disabled={disabled || !isIncluded}
                checked={isIncluded ? nullableEnabled : false}
                onCheckedChange={(state: boolean) =>
                  handleNullableEnabledChange(column.name, state)
                }
              />
            )}
          </Fragment>
        );
      })}
    </div>
  );
}