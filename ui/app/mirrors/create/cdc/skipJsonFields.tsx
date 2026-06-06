'use client';
import {
  Dispatch,
  SetStateAction,
  useCallback,
  useMemo,
  useState,
} from 'react';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { Checkbox } from '@/lib/Checkbox';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';

interface SkipJsonFieldsProps {
  tableRow: TableMapRow;
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

const MONGODB_DOC_COLUMN_PREFIX = 'doc.';

export default function SkipJsonFields({
  tableRow,
  setRows,
}: SkipJsonFieldsProps) {
  const existingExclude = useMemo(() => {
    return Array.from(tableRow.exclude)
      .filter((field) => field.startsWith(MONGODB_DOC_COLUMN_PREFIX))
      .map((field) => field.slice(MONGODB_DOC_COLUMN_PREFIX.length))
      .join(',');
  }, [tableRow.exclude]);

  const [showExclude, setShowExclude] = useState(
    Array.from(tableRow.exclude).some((f) =>
      f.startsWith(MONGODB_DOC_COLUMN_PREFIX)
    )
  );
  const [excludeValue, setExcludeValue] = useState(existingExclude);

  const updateExclude = useCallback(
    (value: string) => {
      const fields = value
        .split(',')
        .map((f) => f.trim())
        .filter((f) => f !== '')
        .map((f) => MONGODB_DOC_COLUMN_PREFIX + f);

      setRows((prev) =>
        prev.map((row) => {
          if (row.source !== tableRow.source) return row;
          return {
            ...row,
            exclude: new Set(fields),
          };
        })
      );
    },
    [tableRow.source, setRows]
  );

  const handleToggle = useCallback(
    (state: boolean) => {
      setShowExclude(state);
      if (!state) {
        setExcludeValue('');
        updateExclude('');
      }
    },
    [updateExclude]
  );

  const handleChange = useCallback(
    (value: string) => {
      setExcludeValue(value);
      updateExclude(value);
    },
    [updateExclude]
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '0.5rem',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        <RowWithCheckbox
          label={
            <Label as='label' style={{ fontSize: 13 }}>
              Hide sensitive fields
            </Label>
          }
          action={
            <Checkbox
              style={{ marginLeft: 0 }}
              checked={showExclude}
              onCheckedChange={handleToggle}
            />
          }
        />
      </div>
      {showExclude && (
        <TextField
          variant='simple'
          placeholder='password,user.ssn,metadata.secret'
          value={excludeValue}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
            handleChange(e.target.value)
          }
        />
      )}
    </div>
  );
}
