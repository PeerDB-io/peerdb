'use client';
import {
  Dispatch,
  SetStateAction,
  useCallback,
  useEffect,
  useState,
} from 'react';
import ReactSelect from 'react-select';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import SelectTheme from '@/app/styles/select';
import { notifySortingKey } from '@/app/utils/notify';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { engineOptionStyles } from './styles';

interface SortingKeyType {
  name: string;
  disabled: boolean;
}

interface SortingKeysProps {
  columns: { value: string; label: string; isPkey: boolean }[];
  tableRow: TableMapRow;
  loading: boolean;
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

const SelectSortingKeys = ({
  columns,
  loading,
  tableRow,
  setRows,
}: SortingKeysProps) => {
  const [sortingKeysSelections, setSortingKeysSelections] = useState<
    SortingKeyType[]
  >([]);
  const [showSortingKey, setShowSortingKey] = useState(false);

  const handleSortingKey = useCallback(
    (col: SortingKeyType, action: 'add' | 'remove') => {
      setSortingKeysSelections((prev) => {
        if (action === 'add' && !prev.some((key) => key.name === col.name)) {
          return [col, ...prev];
        } else if (action === 'remove' && !col.disabled) {
          return prev.filter((prevCol) => prevCol.name !== col.name);
        }
        return prev;
      });
    },
    []
  );

  const registerSortingKeys = useCallback(() => {
    setRows((prevRows) => {
      const rowIndex = prevRows.findIndex(
        (row) => row.source === tableRow.source
      );
      if (rowIndex !== -1) {
        const newColumns = prevRows[rowIndex].columns.map((col) => ({
          ...col,
          ordering:
            sortingKeysSelections.findIndex(
              (key) => key.name === col.sourceName
            ) + 1,
        }));
        sortingKeysSelections.forEach((sortingKeyCol, orderingIndex) => {
          if (
            !newColumns.some((col) => col.sourceName === sortingKeyCol.name)
          ) {
            newColumns.push({
              sourceName: sortingKeyCol.name,
              destinationName: '',
              destinationType: '',
              ordering: orderingIndex + 1,
              nullableEnabled: false,
            });
          }
        });
        const newRows = [...prevRows];
        newRows[rowIndex].columns = newColumns;
        return newRows;
      }
      return prevRows;
    });
  }, [sortingKeysSelections, setRows, tableRow.source]);

  const handleShowSortingKey = useCallback(
    (state: boolean) => {
      setShowSortingKey(state);
      if (!state) {
        setSortingKeysSelections([]);
        registerSortingKeys();
      } else {
        notifySortingKey();
      }
    },
    [registerSortingKeys]
  );

  useEffect(() => {
    if (showSortingKey && columns.length > 0) {
      setSortingKeysSelections((prev) => {
        if (prev.length === 0) {
          return columns
            .filter((col) => col.isPkey)
            .map((col) => ({
              name: col.label,
              disabled: true,
            }));
        }
        return prev;
      });
    }
  }, [columns, showSortingKey]);

  useEffect(() => {
    if (showSortingKey) {
      registerSortingKeys();
    }
  }, [registerSortingKeys, showSortingKey]);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignContent: 'center',
      }}
    >
      <ToastContainer containerId={'sorting_key_warning'} />
      <RowWithCheckbox
        label={
          <Label as='label' style={{ fontSize: 13 }}>
            Use a custom sorting key
          </Label>
        }
        action={
          <Checkbox
            style={{ marginLeft: 0 }}
            checked={showSortingKey}
            onCheckedChange={handleShowSortingKey}
          />
        }
      />
      {showSortingKey && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignContent: 'center',
          }}
        >
          <ReactSelect
            menuPlacement='top'
            placeholder={'Select sorting keys'}
            onChange={(val, action) => {
              val &&
                handleSortingKey(
                  { name: val.value, disabled: val.isPkey },
                  'add'
                );
            }}
            isOptionDisabled={(option) =>
              option?.isPkey ||
              sortingKeysSelections.findIndex(
                (key) => key.name === option?.label
              ) !== -1
            }
            isLoading={loading}
            value={null}
            styles={engineOptionStyles}
            options={columns}
            theme={SelectTheme}
            isClearable
          />
          <div
            style={{
              display: 'flex',
              marginTop: '0.5rem',
              columnGap: '0.5rem',
              rowGap: '0.5rem',
              alignItems: 'center',
              flexWrap: 'wrap',
            }}
          >
            {sortingKeysSelections.map((col: SortingKeyType) => {
              return (
                <div
                  key={col.name}
                  style={{
                    display: 'flex',
                    columnGap: '0.3rem',
                    alignItems: 'center',
                    border: '1px solid #e5e7eb',
                    borderRadius: '1rem',
                    paddingLeft: '0.5rem',
                    paddingRight: '0.5rem',
                  }}
                >
                  <p style={{ fontSize: '0.7rem' }}>{col.name}</p>
                  <Button
                    variant='normalBorderless'
                    onClick={() => handleSortingKey(col, 'remove')}
                    style={{ padding: 0 }}
                    disabled={col.disabled}
                  >
                    <Icon name='close' />
                  </Button>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default SelectSortingKeys;
