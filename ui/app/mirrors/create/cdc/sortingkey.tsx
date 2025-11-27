'use client';
import {
  Dispatch,
  SetStateAction,
  useCallback,
  useMemo,
  useState,
} from 'react';
import ReactSelect from 'react-select';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import SelectTheme from '@/app/styles/select';
import { notifySortingKey } from '@/app/utils/notify';
import { ColumnSetting } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { ToastContainer } from 'react-toastify';
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

function SortedSelection(
  tableRow: TableMapRow,
  valueFunc: (setting: ColumnSetting) => number
): ColumnSetting[] {
  const filtered = tableRow.columns.filter((col) => valueFunc(col) > 0);
  filtered.sort((r1, r2) => valueFunc(r1) - valueFunc(r2));
  return filtered;
}

function UpdatedRows(
  rows: TableMapRow[],
  tableRow: TableMapRow,
  colName: string,
  update: (setting: ColumnSetting) => ColumnSetting
): TableMapRow[] {
  const rowIndex = rows.findIndex((row) => row.source === tableRow.source);
  if (rowIndex !== -1) {
    const newRows = [...rows];
    // Check if there's an existing ColumnSetting to preserve nullableEnabled
    const existingSetting = rows[rowIndex].columns.find(
      (setting) => setting.sourceName === colName
    );
    const columns = rows[rowIndex].columns.map(update);
    if (!columns.find((setting) => setting.sourceName === colName)) {
      columns.push(
        update({
          sourceName: colName,
          destinationName: '',
          destinationType: '',
          ordering: 0,
          partitioning: 0,
          nullableEnabled: existingSetting?.nullableEnabled ?? false,
        })
      );
    }
    newRows[rowIndex] = { ...rows[rowIndex], columns };
    return newRows;
  }
  return rows;
}

export default function SelectSortingKeys({
  columns,
  loading,
  tableRow,
  setRows,
}: SortingKeysProps) {
  const sortingKeysSelections = useMemo(
    () => SortedSelection(tableRow, (setting) => setting.ordering),
    [tableRow]
  );
  const [showSortingKey, setShowSortingKey] = useState(false);

  const partitioningKeysSelections = useMemo(
    () => SortedSelection(tableRow, (setting) => setting.partitioning),
    [tableRow]
  );
  const [showPartitioningKey, setShowPartitioningKey] = useState(false);

  const handleAddSortingKey = useCallback(
    (col: string) => {
      if (
        !sortingKeysSelections.find((setting) => setting.sourceName === col)
      ) {
        setRows((rows) =>
          UpdatedRows(rows, tableRow, col, (setting) => ({
            ...setting,
            ordering:
              setting.sourceName === col
                ? sortingKeysSelections.length + 1
                : sortingKeysSelections.indexOf(setting) + 1,
          }))
        );
      }
    },
    [sortingKeysSelections, setRows, tableRow]
  );

  const handleRemoveSortingKey = useCallback(
    (col: ColumnSetting) => {
      const removedIndex = sortingKeysSelections.findIndex(
        (setting) => setting === col
      );
      if (removedIndex !== -1) {
        setRows((rows) =>
          UpdatedRows(rows, tableRow, col.sourceName, (setting) => {
            if (setting === col) {
              return { ...setting, ordering: 0 };
            }
            const oldIndex = sortingKeysSelections.indexOf(setting);
            return {
              ...setting,
              ordering: oldIndex + (oldIndex > removedIndex ? 2 : 1),
            };
          })
        );
      }
    },
    [sortingKeysSelections, setRows, tableRow]
  );

  const handleAddPartitioningKey = useCallback(
    (col: string) => {
      if (
        !partitioningKeysSelections.find(
          (setting) => setting.sourceName === col
        )
      ) {
        setRows((rows) =>
          UpdatedRows(rows, tableRow, col, (setting) => ({
            ...setting,
            partitioning:
              setting.sourceName === col
                ? partitioningKeysSelections.length + 1
                : partitioningKeysSelections.indexOf(setting) + 1,
          }))
        );
      }
    },
    [partitioningKeysSelections, setRows, tableRow]
  );

  const handleRemovePartitioningKey = useCallback(
    (col: ColumnSetting) => {
      const removedIndex = partitioningKeysSelections.findIndex(
        (setting) => setting === col
      );
      if (removedIndex !== -1) {
        setRows((rows) =>
          UpdatedRows(rows, tableRow, col.sourceName, (setting) => {
            if (setting === col) {
              return { ...setting, partitioning: 0 };
            }
            const oldIndex = partitioningKeysSelections.indexOf(setting);
            return {
              ...setting,
              partitioning: oldIndex + (oldIndex > removedIndex ? 2 : 1),
            };
          })
        );
      }
    },
    [partitioningKeysSelections, setRows, tableRow]
  );

  const handleShowSortingKey = useCallback(
    (state: boolean) => {
      setShowSortingKey(state);
      if (!state) {
        setRows((prevRows) =>
          prevRows.map((row) => ({
            ...row,
            columns: row.columns.map((column) => ({ ...column, ordering: 0 })),
          }))
        );
      } else {
        notifySortingKey();
      }
    },
    [setRows]
  );

  const handleShowPartitioningKey = useCallback(
    (state: boolean) => {
      setShowPartitioningKey(state);
      if (!state) {
        setRows((prevRows) =>
          prevRows.map((row) => ({
            ...row,
            columns: row.columns.map((column) => ({
              ...column,
              partitioning: 0,
            })),
          }))
        );
      } else {
        // TODO notifyPartitioningKey();
      }
    },
    [setRows]
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignContent: 'center',
        rowGap: '0.5rem',
      }}
    >
      <ToastContainer containerId='sorting_key_warning' />
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
            placeholder='Select sorting keys'
            onChange={(val, _action) => {
              if (val) handleAddSortingKey(val.value);
            }}
            isLoading={loading}
            value={null}
            styles={engineOptionStyles}
            options={columns.map((col) => ({ value: col, label: col }))}
            theme={SelectTheme}
            isClearable
          />
          <div style={sortingKeyPillContainerStyle}>
            {sortingKeysSelections.map((col: ColumnSetting) => (
              <div key={col.sourceName} style={sortingKeyPillStyle}>
                <p style={{ fontSize: '0.7rem' }}>{col.sourceName}</p>
                <Button
                  variant='normalBorderless'
                  onClick={() => handleRemoveSortingKey(col)}
                  style={{ padding: 0 }}
                >
                  <Icon name='close' />
                </Button>
              </div>
            ))}
          </div>
        </div>
      )}
      <RowWithCheckbox
        label={
          <Label as='label' style={{ fontSize: 13 }}>
            Partition ClickHouse table
          </Label>
        }
        action={
          <Checkbox
            style={{ marginLeft: 0 }}
            checked={showPartitioningKey}
            onCheckedChange={handleShowPartitioningKey}
          />
        }
      />
      {showPartitioningKey && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignContent: 'center',
          }}
        >
          <ReactSelect
            menuPlacement='top'
            placeholder='Select partitioning columns'
            onChange={(val, _action) => {
              if (val) handleAddPartitioningKey(val.value);
            }}
            isLoading={loading}
            value={null}
            styles={engineOptionStyles}
            options={columns.map((col) => ({ value: col, label: col }))}
            theme={SelectTheme}
            isClearable
          />
          <div style={sortingKeyPillContainerStyle}>
            {partitioningKeysSelections.map((col: ColumnSetting) => (
              <div key={col.sourceName} style={sortingKeyPillStyle}>
                <p style={{ fontSize: '0.7rem' }}>{col.sourceName}</p>
                <Button
                  variant='normalBorderless'
                  onClick={() => handleRemovePartitioningKey(col)}
                  style={{ padding: 0 }}
                >
                  <Icon name='close' />
                </Button>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
