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
import {
  engineOptionStyles,
  sortingKeyPillContainerStyle,
  sortingKeyPillStyle,
} from './styles';

interface SortingKeysProps {
  columns: string[];
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
  const [sortingKeysSelections, setSortingKeysSelections] = useState<string[]>(
    []
  );
  const [showSortingKey, setShowSortingKey] = useState(false);

  const handleSortingKey = useCallback(
    (col: string, action: 'add' | 'remove') => {
      setSortingKeysSelections((prev) => {
        if (action === 'add' && !prev.some((key) => key === col)) {
          return [...prev, col];
        } else if (action === 'remove') {
          return prev.filter((prevCol) => prevCol !== col);
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
            sortingKeysSelections.findIndex((key) => key === col.sourceName) +
            1,
        }));
        sortingKeysSelections.forEach((sortingKeyCol, orderingIndex) => {
          if (!newColumns.some((col) => col.sourceName === sortingKeyCol)) {
            newColumns.push({
              sourceName: sortingKeyCol,
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
        rowGap: '0.5rem',
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
              val && handleSortingKey(val.value, 'add');
            }}
            isLoading={loading}
            value={null}
            styles={engineOptionStyles}
            options={columns.map((col) => ({ value: col, label: col }))}
            theme={SelectTheme}
            isClearable
          />
          <div style={sortingKeyPillContainerStyle}>
            {sortingKeysSelections.map((col: string) => {
              return (
                <div key={col} style={sortingKeyPillStyle}>
                  <p style={{ fontSize: '0.7rem' }}>{col}</p>
                  <Button
                    variant='normalBorderless'
                    onClick={() => handleSortingKey(col, 'remove')}
                    style={{ padding: 0 }}
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
