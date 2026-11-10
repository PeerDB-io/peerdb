'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { ColumnsItem } from '@/grpc_generated/route';
import { Checkbox } from '@/lib/Checkbox';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { Dispatch, SetStateAction } from 'react';
import { useTheme } from 'styled-components';

interface ColumnProps {
  columns: ColumnsItem[];
  tableRow: TableMapRow;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  disabled?: boolean;
  showOrdering: boolean;
  compact?: boolean;
}

// Postgres spells some types out; shorten them so a row stays on one line.
const shortType = (t: string) =>
  t
    .replace('timestamp without time zone', 'timestamp')
    .replace('timestamp with time zone', 'timestamptz')
    .replace('character varying', 'varchar')
    .replace('double precision', 'float8');
export default function ColumnBox({
  columns,
  tableRow,
  rows,
  setRows,
  disabled,
  compact,
}: ColumnProps) {
  const theme = useTheme();
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

  // compact view: key columns first, in key order (from the table list, else the column flags)
  const keyOrder = tableRow.keyColumns?.length
    ? tableRow.keyColumns
    : columns.filter((c) => c.isKey).map((c) => c.name);
  const keyPos = (name: string) => {
    const i = keyOrder.indexOf(name);
    return i === -1 ? keyOrder.length : i;
  };
  const ordered = compact
    ? [...columns].sort((a, b) => keyPos(a.name) - keyPos(b.name))
    : columns;

  return ordered.map((column) => {
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

    const checkbox = (
      <Checkbox
        style={{ cursor: 'pointer', margin: compact ? 0 : undefined }}
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
    );
    if (compact) {
      return (
        <label
          key={column.name}
          title={`${column.name} · ${column.type}`}
          style={{
            display: 'grid',
            gridTemplateColumns: '20px minmax(0,1fr) auto',
            alignItems: 'center',
            columnGap: 10,
            minHeight: 34,
            fontSize: 13,
          }}
        >
          {checkbox}
          <span
            style={{
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {column.name}
            {keyOrder.includes(column.name) && (
              <span
                title='Primary key column'
                style={{
                  marginLeft: 6,
                  fontSize: 11,
                  fontWeight: 600,
                  padding: '1px 5px',
                  borderRadius: 4,
                  color: theme.colors.accent.text.lowContrast,
                  background: theme.colors.accent.surface.normal,
                }}
              >
                PK{keyOrder.length > 1 ? ` ${keyOrder.indexOf(column.name) + 1}` : ''}
              </span>
            )}
          </span>
          <Label
            as='span'
            colorName='lowContrast'
            style={{
              fontSize: 12.5,
              padding: 0,
              maxWidth: 150,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {shortType(column.type)}
          </Label>
        </label>
      );
    }
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
