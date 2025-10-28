'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import {
  TableEngine,
  tableEngineFromJSON,
  tableEngineToJSON,
  TableMapping,
} from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { ColumnsItem } from '@/grpc_generated/route';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { SearchField } from '@/lib/SearchField';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import {
  Dispatch,
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from 'react';
import ReactSelect from 'react-select';
import { BarLoader } from 'react-spinners/';
import { fetchColumns, fetchTables } from '../handlers';
import ColumnBox from './columnbox';
import CustomColumnType from './customColumnType';
import SchemaSettings from './schemasettings';
import SelectSortingKeys from './sortingkey';
import {
  columnBoxDividerStyle,
  engineOptionStyles,
  expandableStyle,
  schemaBoxStyle,
  tableBoxStyle,
  tooltipStyle,
} from './styles';
interface SchemaBoxProps {
  sourcePeer: string;
  schema: string;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  tableColumns: { tableName: string; columns: ColumnsItem[] }[];
  setTableColumns: Dispatch<
    SetStateAction<{ tableName: string; columns: ColumnsItem[] }[]>
  >;
  peerType?: DBType;
  alreadySelectedTables: TableMapping[] | undefined;
  initialLoadOnly?: boolean;
}

export default function SchemaBox({
  sourcePeer,
  peerType,
  schema,
  rows,
  setRows,
  tableColumns,
  setTableColumns,
  alreadySelectedTables,
  initialLoadOnly,
}: SchemaBoxProps) {
  const [tablesLoading, setTablesLoading] = useState(false);
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [expandedSchemas, setExpandedSchemas] = useState<string[]>([]);
  const [tableQuery, setTableQuery] = useState<string>('');
  const [defaultTargetSchema, setDefaultTargetSchema] =
    useState<string>(schema);
  const searchedTables = useMemo(() => {
    const tableQueryLower = tableQuery.toLowerCase();
    return rows
      .filter(
        (row) =>
          row.schema === schema &&
          row.source.toLowerCase().includes(tableQueryLower)
      )
      .sort((a, b) => a.source.localeCompare(b.source));
  }, [schema, rows, tableQuery]);

  const schemaIsExpanded = useCallback(
    (schema: string) => {
      return expandedSchemas.some((schemaName) => schemaName === schema);
    },
    [expandedSchemas]
  );

  const handleRemoveRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index] = { ...newRows[index], selected: false };
    setRows(newRows);
    removeTableColumns(source);
  };

  const handleTableSelect = (on: boolean, source: string) => {
    on ? handleAddRow(source) : handleRemoveRow(source);
  };

  const updateDestination = (source: string, destination: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], destination };
    setRows(newRows);
  };

  const updatePartitionKey = (source: string, partitionKey: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], partitionKey };
    setRows(newRows);
  };

  const updateEngine = (source: string, engine: TableEngine) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], engine };
    setRows(newRows);
  };

  const updateShardingKey = (source: string, shardingKey: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], shardingKey };
    setRows(newRows);
  };

  const updatePolicyName = (source: string, policyName: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], policyName };
    setRows(newRows);
  };

  const updatePartitionByExpr = (source: string, partitionByExpr: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], partitionByExpr };
    setRows(newRows);
  };

  const addTableColumns = useCallback(
    (table: string) => {
      const [schemaName, tableName] = table.split('.');

      fetchColumns(sourcePeer, schemaName, tableName, setColumnsLoading).then(
        (res) => {
          setTableColumns((prev) => [
            ...prev,
            { tableName: table, columns: res },
          ]);
        }
      );
    },
    [sourcePeer, setTableColumns]
  );

  const handleAddRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index] = { ...newRows[index], selected: true };
    setRows(newRows);
    addTableColumns(source);
  };

  const removeTableColumns = (table: string) => {
    setTableColumns((prev) => {
      return prev.filter((column) => column.tableName !== table);
    });
  };

  const getTableColumns = (tableName: string) => {
    return tableColumns?.find((column) => column.tableName === tableName)
      ?.columns;
  };

  const handleSelectAll = (
    e: React.MouseEvent<HTMLInputElement, MouseEvent>,
    schemaName: string
  ) => {
    const newRows = [...rows];
    for (let i = 0; i < newRows.length; i++) {
      const row = newRows[i];
      if (row.schema === schemaName && row.canMirror) {
        newRows[i] = { ...row, selected: e.currentTarget.checked };
        if (e.currentTarget.checked) addTableColumns(row.source);
        else removeTableColumns(row.source);
      }
    }
    setRows(newRows);
  };

  const handleSchemaClick = (schemaName: string) => {
    if (!schemaIsExpanded(schemaName)) {
      setExpandedSchemas((curr) => [...curr, schemaName]);
    } else {
      setExpandedSchemas((curr) =>
        curr.filter((expandedSchema) => expandedSchema != schemaName)
      );
    }
  };

  const fetchTablesForSchema = useCallback(
    (schemaName: string) => {
      setTablesLoading(true);
      fetchTables(
        sourcePeer,
        schemaName,
        defaultTargetSchema,
        peerType,
        initialLoadOnly
      ).then((newRows) => {
        if (alreadySelectedTables) {
          for (const row of newRows) {
            const existingRow = alreadySelectedTables.find(
              (tableMap) => tableMap.sourceTableIdentifier === row.source
            );
            if (existingRow) {
              row.selected = true;
              row.editingDisabled = true;
              row.engine = existingRow.engine;
              row.partitionKey = existingRow.partitionKey;
              row.shardingKey = existingRow.shardingKey;
              row.policyName = existingRow.policyName;
              row.partitionByExpr = existingRow.partitionByExpr;
              row.exclude = new Set(existingRow.exclude ?? []);
              row.destination = existingRow.destinationTableIdentifier;
              addTableColumns(row.source);
            }
          }
        }
        setRows((oldRows) => {
          const filteredRows = oldRows.filter(
            (oldRow) => oldRow.schema !== schemaName
          );
          return [...filteredRows, ...newRows];
        });
        setTablesLoading(false);
      });
    },
    [
      setRows,
      sourcePeer,
      defaultTargetSchema,
      peerType,
      alreadySelectedTables,
      addTableColumns,
      initialLoadOnly,
    ]
  );

  const engineOptions = [
    { value: 'CH_ENGINE_REPLACING_MERGE_TREE', label: 'ReplacingMergeTree' },
    { value: 'CH_ENGINE_MERGE_TREE', label: 'MergeTree' },
    { value: 'CH_ENGINE_COALESCING_MERGE_TREE', label: 'CoalescingMergeTree' },
    { value: 'CH_ENGINE_NULL', label: 'Null' },
  ];

  useEffect(() => {
    if (schemaIsExpanded(schema)) {
      fetchTablesForSchema(schema);
    }
  }, [schema, fetchTablesForSchema, schemaIsExpanded, initialLoadOnly]);

  return (
    <div style={schemaBoxStyle}>
      <div>
        <div style={{ ...expandableStyle, cursor: 'auto' }}>
          <div
            style={{ display: 'flex', cursor: 'pointer' }}
            onClick={() => handleSchemaClick(schema)}
          >
            <Icon
              name={
                schemaIsExpanded(schema) ? 'arrow_drop_down' : 'arrow_right'
              }
            />
            <p>{schema}</p>
          </div>
          <div style={{ display: schemaIsExpanded(schema) ? 'flex' : 'none' }}>
            <div style={{ display: 'flex' }}>
              <input
                type='checkbox'
                onClick={(e) => handleSelectAll(e, schema)}
              />
              <Label as='label' style={{ fontSize: 14 }}>
                Select All
              </Label>
            </div>
            <SearchField
              style={{ fontSize: 13 }}
              placeholder='Search for tables'
              value={tableQuery}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setTableQuery(e.target.value)
              }
            />
            <div style={{ alignSelf: 'center', cursor: 'pointer' }}>
              <SchemaSettings
                schema={defaultTargetSchema}
                setTargetSchemaOverride={setDefaultTargetSchema}
              />
            </div>
          </div>
        </div>
        {/* TABLE BOX */}
        {schemaIsExpanded(schema) && (
          <div className='ml-5 mt-3'>
            {searchedTables.length ? (
              searchedTables.map((row) => {
                const columns = getTableColumns(row.source);
                return (
                  <div key={row.source} style={tableBoxStyle}>
                    <div
                      className='ml-5'
                      style={{
                        display: 'flex',
                        flexDirection: 'column',
                        rowGap: '1rem',
                      }}
                    >
                      <RowWithCheckbox
                        label={
                          <Tooltip
                            style={{
                              ...tooltipStyle,
                              display: row.canMirror ? 'none' : 'block',
                            }}
                            content={
                              'This table must have a primary key or replica identity to be mirrored.'
                            }
                          >
                            <Label
                              as='label'
                              style={{
                                fontSize: 13,
                                color: row.canMirror ? undefined : 'gray',
                              }}
                            >
                              {row.source}
                            </Label>
                            <Label
                              as='label'
                              colorName='lowContrast'
                              style={{ fontSize: 13 }}
                            >
                              {row.tableSize}
                            </Label>
                          </Tooltip>
                        }
                        action={
                          <Checkbox
                            disabled={!row.canMirror}
                            checked={row.selected}
                            onCheckedChange={(state: boolean) =>
                              handleTableSelect(state, row.source)
                            }
                          />
                        }
                      />
                      <div
                        style={{
                          rowGap: '0.5rem',
                          columnGap: '3rem',
                          display: row.selected ? 'flex' : 'none',
                          flexWrap: 'wrap',
                        }}
                        key={row.source}
                      >
                        <div style={{ width: '30%', fontSize: 12 }}>
                          Target Table:
                          <TextField
                            disabled={row.editingDisabled}
                            style={{
                              marginTop: '0.5rem',
                              cursor: 'pointer',
                            }}
                            variant='simple'
                            placeholder='Enter target table'
                            value={row.destination}
                            onChange={(
                              e: React.ChangeEvent<HTMLInputElement>
                            ) => updateDestination(row.source, e.target.value)}
                          />
                        </div>

                        <div style={{ width: '30%', fontSize: 12 }}>
                          Custom Partitioning Key:
                          <TextField
                            disabled={row.editingDisabled}
                            style={{
                              marginTop: '0.5rem',
                              cursor: 'pointer',
                            }}
                            variant='simple'
                            placeholder='Enter optional custom partiton key'
                            value={row.partitionKey}
                            onChange={(
                              e: React.ChangeEvent<HTMLInputElement>
                            ) => updatePartitionKey(row.source, e.target.value)}
                          />
                        </div>

                        {peerType?.toString() ===
                          DBType[DBType.CLICKHOUSE].toString() && (
                          <>
                            <div style={{ width: '30%', fontSize: 12 }}>
                              Engine:
                              <ReactSelect
                                isDisabled={row.editingDisabled}
                                styles={engineOptionStyles}
                                options={engineOptions}
                                value={
                                  engineOptions.find(
                                    (x) =>
                                      x.value ===
                                      (typeof row.engine === 'string'
                                        ? row.engine
                                        : tableEngineToJSON(row.engine))
                                  ) ?? engineOptions[0]
                                }
                                onChange={(selectedOption) =>
                                  selectedOption &&
                                  updateEngine(
                                    row.source,
                                    tableEngineFromJSON(selectedOption.value)
                                  )
                                }
                              />
                            </div>
                            <div style={{ width: '30%', fontSize: 12 }}>
                              Sharding Key:
                              <TextField
                                disabled={row.editingDisabled}
                                style={{
                                  marginTop: '0.5rem',
                                  cursor: 'pointer',
                                }}
                                variant='simple'
                                placeholder='Sharding key expression (optional)'
                                value={row.shardingKey}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) =>
                                  updateShardingKey(row.source, e.target.value)
                                }
                              />
                            </div>
                            <div style={{ width: '30%', fontSize: 12 }}>
                              Policy Name:
                              <TextField
                                disabled={row.editingDisabled}
                                style={{
                                  marginTop: '0.5rem',
                                  cursor: 'pointer',
                                }}
                                variant='simple'
                                placeholder='Policy name (optional)'
                                value={row.policyName}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) =>
                                  updatePolicyName(row.source, e.target.value)
                                }
                              />
                            </div>
                            <div style={{ width: '30%', fontSize: 12 }}>
                              Partition By Expr:
                              <TextField
                                disabled={row.editingDisabled}
                                style={{
                                  marginTop: '0.5rem',
                                  cursor: 'pointer',
                                }}
                                variant='simple'
                                placeholder='Partition By expression (optional)'
                                value={row.partitionByExpr}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) =>
                                  updatePartitionByExpr(
                                    row.source,
                                    e.target.value
                                  )
                                }
                              />
                            </div>
                          </>
                        )}
                      </div>
                    </div>

                    {/* COLUMN BOX */}
                    {row.selected && (
                      <div className='ml-5 mt-3' style={{ width: '100%' }}>
                        <hr style={columnBoxDividerStyle} />
                        <div
                          style={{
                            display: 'flex',
                            flexDirection: 'column',
                            rowGap: '0.5rem',
                            width: '100%',
                          }}
                        >
                          <Label
                            as='label'
                            colorName='lowContrast'
                            style={{ fontSize: 13 }}
                          >
                            Columns
                          </Label>
                        </div>
                        {columns ? (
                          <>
                            <ColumnBox
                              columns={columns}
                              tableRow={row}
                              rows={rows}
                              setRows={setRows}
                              disabled={row.editingDisabled}
                              showOrdering={
                                peerType?.toString() ===
                                DBType[DBType.CLICKHOUSE].toString()
                              }
                            />
                            {peerType?.toString() ===
                              DBType[DBType.CLICKHOUSE].toString() && (
                              <div
                                style={{
                                  width: '100%',
                                  display: 'flex',
                                  flexDirection: 'column',
                                  rowGap: '0.5rem',
                                }}
                              >
                                <hr style={columnBoxDividerStyle} />
                                <div style={{ width: '50%' }}>
                                  <SelectSortingKeys
                                    columns={columns
                                      .map((column) => column.name)
                                      .filter((name) => !row.exclude.has(name))}
                                    loading={columnsLoading}
                                    tableRow={row}
                                    setRows={setRows}
                                  />
                                </div>
                                <CustomColumnType
                                  columns={columns}
                                  tableRow={row}
                                  rows={rows}
                                  setRows={setRows}
                                  peerType={peerType}
                                />
                              </div>
                            )}
                          </>
                        ) : columnsLoading ? (
                          <BarLoader />
                        ) : (
                          <Label
                            as='label'
                            colorName='lowContrast'
                            style={{ fontSize: 13 }}
                          >
                            No columns in {row.source}
                          </Label>
                        )}
                      </div>
                    )}
                  </div>
                );
              })
            ) : tablesLoading ? (
              <BarLoader />
            ) : (
              <Label
                as='label'
                colorName='lowContrast'
                style={{ fontSize: 13 }}
              >
                No tables in {schema}
              </Label>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
