'use client';
import { Divider } from '@tremor/react';
import {
  Dispatch,
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from 'react';
import ReactSelect from 'react-select';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import SelectTheme from '@/app/styles/select';
import { DBType } from '@/grpc_generated/peers';
import { ColumnsItem } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { fetchAllTypeConversions } from '../handlers';
import { columnBoxDividerStyle, engineOptionStyles } from './styles';
interface CustomColumnTypeProps {
  columns: ColumnsItem[];
  tableRow: TableMapRow;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  peerType: DBType;
}

export default function CustomColumnType({
  columns,
  tableRow,
  rows,
  setRows,
  peerType,
}: CustomColumnTypeProps) {
  const [useCustom, setUseCustom] = useState(false);
  const [selectedColumnName, setSelectedColumnName] = useState<string>('');
  const [destinationTypeMapping, setDestinationTypeMapping] = useState<
    Record<string, { value: string; label: string }[]>
  >({});

  const selectedColumns = useMemo(() => {
    return columns.filter((col) => !tableRow.exclude.has(col.name));
  }, [columns, tableRow.exclude]);

  const columnsWithDstTypes = useMemo(() => {
    const columnsWithDstTypes = selectedColumns.filter(
      (col) => destinationTypeMapping[col.name] !== undefined
    );
    if (columnsWithDstTypes.length == 0) {
      setUseCustom(false);
    }
    return columnsWithDstTypes;
  }, [selectedColumns, destinationTypeMapping]);

  const customColumnTypeList = useMemo(() => {
    const currentRow = rows.find((r) => r.source === tableRow.source);
    if (!currentRow) return [];

    return currentRow.columns
      .filter((col) => col.destinationType)
      .map((col) => ({
        columnName: col.sourceName,
        destinationType: col.destinationType,
      }));
  }, [rows, tableRow.source]);

  const remainingColumnsWithDstTypes = columnsWithDstTypes.filter(
    (col) =>
      !customColumnTypeList.some((mapping) => mapping.columnName === col.name)
  );

  const destinationTypeOptions = useCallback(
    (col: string): { value: string; label: string }[] => {
      return destinationTypeMapping[col] ?? [];
    },
    [destinationTypeMapping]
  );

  useEffect(() => {
    const fetchTypeMappings = async () => {
      const columnNameToQvalueKind = selectedColumns.map((col) => [
        col.name,
        col.qkind,
      ]) as [string, string][];

      try {
        const typeConversions = await fetchAllTypeConversions(peerType);
        const columnNameToDestinationTypes = {} as Record<
          string,
          { value: string; label: string }[]
        >;
        for (const [columnName, qkind] of columnNameToQvalueKind) {
          for (const tc of typeConversions) {
            if (tc.qkind === qkind) {
              columnNameToDestinationTypes[columnName] =
                tc.destinationTypes.map((type: string) => ({
                  value: type,
                  label: type,
                })) ?? [];
              break;
            }
          }
        }
        setDestinationTypeMapping(columnNameToDestinationTypes);
      } catch (error) {
        console.error('Error fetching type conversions:', error);
      }
    };

    fetchTypeMappings();
  }, [peerType, selectedColumns]);

  const handleUseCustomDstType = useCallback(
    (state: boolean) => {
      setUseCustom(state);
      if (!state) {
        setSelectedColumnName('');

        // clear custom column types already set
        setRows((prev) =>
          prev.map((row) => {
            if (row.source !== tableRow.source) return row;
            return {
              ...row,
              columns: row.columns.map((col) => ({
                ...col,
                destinationType: '',
              })),
            };
          })
        );
      }
    },
    [tableRow.source]
  );

  const handleCreateColumn = useCallback(
    (value: string) => {
      if (selectedColumnName) {
        setRows((prev) =>
          prev.map((row) => {
            if (row.source !== tableRow.source) return row;

            const columnExistsInRow = row.columns.some(
              (col) => col.sourceName === selectedColumnName
            );
            if (!columnExistsInRow) {
              return {
                ...row,
                columns: [
                  ...row.columns,
                  {
                    sourceName: selectedColumnName,
                    destinationName: '',
                    destinationType: value,
                    ordering: 0,
                    nullableEnabled: false,
                  },
                ],
              };
            } else {
              return {
                ...row,
                columns: row.columns.map((col) =>
                  col.sourceName === selectedColumnName
                    ? { ...col, destinationType: value }
                    : col
                ),
              };
            }
          })
        );
        setSelectedColumnName('');
      }
    },
    [tableRow.source, setRows, selectedColumnName]
  );

  const handleUpdateColumn = useCallback(
    (entry: { columnName: string; destinationType: string }, value: string) => {
      setRows((prev) =>
        prev.map((row) => {
          if (row.source !== tableRow.source) return row;

          if (value === '') {
            return {
              ...row,
              columns: row.columns.filter(
                (col) => col.sourceName !== entry.columnName
              ),
            };
          } else {
            return {
              ...row,
              columns: row.columns.map((col) =>
                col.sourceName === entry.columnName
                  ? { ...col, destinationType: value }
                  : col
              ),
            };
          }
        })
      );
    },
    [tableRow.source, setRows]
  );

  return (
    columnsWithDstTypes.length > 0 && (
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignContent: 'center',
          rowGap: '0.5rem',
        }}
      >
        <Divider
          style={{
            ...columnBoxDividerStyle,
            marginTop: '0.5rem',
          }}
        />
        <RowWithCheckbox
          label={
            <Label as='label' style={{ fontSize: 13 }}>
              Use custom destination column type
            </Label>
          }
          action={
            <Checkbox
              style={{ marginLeft: 0 }}
              checked={useCustom}
              onCheckedChange={handleUseCustomDstType}
            />
          }
        />
        {useCustom && (
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: '0.5rem',
            }}
          >
            {customColumnTypeList.length > 0 && (
              <div
                style={{
                  display: 'flex',
                  gap: '0.5rem',
                  flexDirection: 'column',
                }}
              >
                {customColumnTypeList.map((mapping) => (
                  <div
                    key={mapping.columnName}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                    }}
                  >
                    <div style={{ width: '30%' }}>
                      <ReactSelect
                        value={{
                          value: mapping.columnName,
                          label: mapping.columnName,
                        }}
                        theme={SelectTheme}
                        styles={engineOptionStyles}
                      />
                    </div>
                    <span>→</span>
                    <div style={{ width: '30%' }}>
                      <ReactSelect
                        value={{
                          value: mapping.destinationType,
                          label: mapping.destinationType,
                        }}
                        onChange={(val) =>
                          val?.value && handleUpdateColumn(mapping, val.value)
                        }
                        theme={SelectTheme}
                        styles={engineOptionStyles}
                        options={destinationTypeOptions(mapping.columnName)}
                      />
                    </div>
                    <Button
                      variant='normalBorderless'
                      onClick={() => handleUpdateColumn(mapping, '')}
                    >
                      <Icon name='close' />
                    </Button>
                  </div>
                ))}
              </div>
            )}

            {remainingColumnsWithDstTypes.length > 0 && (
              <div
                style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}
              >
                <div style={{ width: '30%' }}>
                  <ReactSelect
                    placeholder='Column'
                    value={
                      selectedColumnName
                        ? {
                            value: selectedColumnName,
                            label: selectedColumnName,
                          }
                        : null
                    }
                    onChange={(val) =>
                      val?.value && setSelectedColumnName(val.value)
                    }
                    options={remainingColumnsWithDstTypes.map((col) => ({
                      value: col.name,
                      label: col.name,
                    }))}
                    theme={SelectTheme}
                    styles={engineOptionStyles}
                  />
                </div>
                <span>→</span>
                <div style={{ width: '30%' }}>
                  <ReactSelect
                    placeholder='Destination type'
                    value={null}
                    onChange={(val) =>
                      val?.value && handleCreateColumn(val.value)
                    }
                    options={
                      selectedColumnName
                        ? destinationTypeOptions(selectedColumnName)
                        : []
                    }
                    theme={SelectTheme}
                    styles={engineOptionStyles}
                  />
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    )
  );
}
