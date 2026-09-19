'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction, useCallback } from 'react';
import {
  COLUMN_TYPE_PATTERN,
  structuredColumnIssues,
} from '../helpers/structured';
import { columnBoxDividerStyle } from './styles';

interface StructuredColumnsProps {
  tableRow: TableMapRow;
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  disabled?: boolean;
}

// StructuredColumns declares the destination schema of a table replicated with structured
// ingestion. Schemaless sources report no columns, so the columns cannot be discovered and
// picked like they are for a relational source: they are typed in here, and the mapping is
// the only place the destination schema exists.
export default function StructuredColumns({
  tableRow,
  setRows,
  disabled,
}: StructuredColumnsProps) {
  const updateColumns = useCallback(
    (update: (columns: TableMapRow['columns']) => TableMapRow['columns']) => {
      setRows((prev) =>
        prev.map((row) =>
          row.source === tableRow.source
            ? { ...row, columns: update(row.columns) }
            : row
        )
      );
    },
    [tableRow.source, setRows]
  );

  const addColumn = useCallback(() => {
    updateColumns((columns) => [
      ...columns,
      {
        sourceName: '',
        // Renames are not supported under structured ingestion: the destination column
        // takes the source field's name.
        destinationName: '',
        destinationType: '',
        ordering: 0,
        partitioning: 0,
        nullableEnabled: false,
      },
    ]);
  }, [updateColumns]);

  const removeColumn = useCallback(
    (index: number) => {
      updateColumns((columns) => columns.filter((_, i) => i !== index));
    },
    [updateColumns]
  );

  const updateSourceName = useCallback(
    (index: number, sourceName: string) => {
      updateColumns((columns) =>
        columns.map((col, i) => (i === index ? { ...col, sourceName } : col))
      );
    },
    [updateColumns]
  );

  const updateDestinationType = useCallback(
    (index: number, destinationType: string) => {
      updateColumns((columns) =>
        columns.map((col, i) =>
          i === index ? { ...col, destinationType } : col
        )
      );
    },
    [updateColumns]
  );

  const issues = structuredColumnIssues(tableRow.columns);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '0.5rem',
        width: '100%',
      }}
    >
      <hr style={{ ...columnBoxDividerStyle, marginTop: '0.5rem' }} />
      <Label as='label' style={{ fontSize: 13 }}>
        Structured columns
      </Label>
      <Label as='label' colorName='lowContrast' style={{ fontSize: 12 }}>
        Fields to project out of each document, with the destination type to
        store them as. Each field lands in a column of the same name. Nested
        fields are addressed with dots, and a type can be wrapped in
        Nullable(...) to accept missing values. Fields left out are not
        replicated.
      </Label>

      {tableRow.columns.map((column, index) => {
        const invalidType =
          column.destinationType !== '' &&
          !COLUMN_TYPE_PATTERN.test(column.destinationType);
        return (
          <div
            key={index}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              width: '100%',
            }}
          >
            <div style={{ width: '30%' }}>
              <TextField
                disabled={disabled}
                variant='simple'
                placeholder='Source field'
                value={column.sourceName}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  updateSourceName(index, e.target.value)
                }
              />
            </div>
            <span>→</span>
            <div style={{ width: '30%' }}>
              <TextField
                disabled={disabled}
                variant='simple'
                placeholder='Destination type'
                value={column.destinationType}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  updateDestinationType(index, e.target.value)
                }
              />
            </div>
            {invalidType && (
              <Label
                as='label'
                colorName='lowContrast'
                style={{ fontSize: 12, color: 'red' }}
              >
                Invalid type
              </Label>
            )}
            {!disabled && (
              <Button
                variant='normalBorderless'
                onClick={() => removeColumn(index)}
              >
                <Icon name='close' />
              </Button>
            )}
          </div>
        );
      })}

      {!disabled && (
        <div style={{ width: '30%' }}>
          <Button variant='normalSolid' onClick={addColumn}>
            Add column
          </Button>
        </div>
      )}

      {issues.length > 0 && (
        <Label as='label' style={{ fontSize: 12, color: 'red' }}>
          {issues[0]}
        </Label>
      )}
    </div>
  );
}
