'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { useSelectTheme } from '@/app/styles/select';
import {
  TableEngine,
  tableEngineFromJSON,
  tableEngineToJSON,
  TableMapping,
} from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { ColumnsItem } from '@/grpc_generated/route';
import { BarLoader } from '@/lib/BarLoader';
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
  useMemo,
  useState,
  useTransition,
} from 'react';
import ReactSelect from 'react-select';
import { useTheme as useStyledTheme } from 'styled-components';
import {
  fetchColumns,
  fetchTables,
  getDefaultDestinationTable,
} from '../handlers';
import {
  structuredIngestionConfig,
  structuredIngestionEnabled,
  withStructuredIngestionConfig,
} from '../helpers/structured';
import ColumnBox from './columnbox';
import CustomColumnType from './customColumnType';
import SchemaSettings from './schemasettings';
import SelectSortingKeys from './sortingkey';
import StructuredColumns from './structuredColumns';
import {
  columnBoxDividerStyle,
  engineOptionStyles,
  expandableStyle,
  schemaBoxStyle,
  tableBoxStyle,
  tooltipStyle,
} from './styles';
function cannotMirrorReason(row: TableMapRow): string {
  const reasons: string[] = [];
  if (!row.hasPrimaryKeyOrReplicaIdentity) {
    reasons.push('It needs a primary key or replica identity.');
  }
  if (row.isUnlogged) {
    reasons.push(
      'It is unlogged, which CDC cannot replicate. Run ALTER TABLE ... SET LOGGED.'
    );
  }
  return `This table cannot be mirrored. ${reasons.join(' ')}`;
}

interface SchemaBoxProps {
  sourcePeer: string;
  schema: string;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  tableColumns: { tableName: string; columns: ColumnsItem[] }[];
  setTableColumns: Dispatch<
    SetStateAction<{ tableName: string; columns: ColumnsItem[] }[]>
  >;
  structuredIngestionSupported: boolean;
  setStructuredIngestionSupported: Dispatch<SetStateAction<boolean>>;
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
  structuredIngestionSupported,
  setStructuredIngestionSupported,
  alreadySelectedTables,
  initialLoadOnly,
}: SchemaBoxProps) {
  const selectTheme = useSelectTheme();
  const styledTheme = useStyledTheme();
  const [tablesLoading, startTablesTransition] = useTransition();
  const [columnsLoading, startColumnsTransition] = useTransition();
  const [expandedSchemas, setExpandedSchemas] = useState<string[]>([]);
  const [fetchedSchemas, setFetchedSchemas] = useState<Set<string>>(new Set());
  const [tableQuery, setTableQuery] = useState<string>('');
  const [defaultTargetSchema, setDefaultTargetSchema] =
    useState<string>(schema);

  const applyTargetSchemaOverride = (newSchema: string) => {
    setDefaultTargetSchema(newSchema);
    if (peerType === undefined) return;
    setRows((oldRows) =>
      oldRows.map((row) =>
        row.schema !== schema || row.editingDisabled
          ? row
          : {
              ...row,
              destination: getDefaultDestinationTable(
                peerType,
                newSchema,
                row.source.slice(schema.length + 1)
              ),
            }
      )
    );
  };
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

  const updateStructuredIngestion = (source: string, enabled: boolean) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    // A table that does not use structured ingestion carries no settings at all
    newRows[index] = withStructuredIngestionConfig(
      newRows[index],
      enabled
        ? {
            enabled,
            dropUnexpectedValues:
              structuredIngestionConfig(newRows[index])?.dropUnexpectedValues ??
              false,
          }
        : undefined
    );
    setRows(newRows);
  };

  const updateDropUnexpectedValues = (
    source: string,
    dropUnexpectedValues: boolean
  ) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = withStructuredIngestionConfig(newRows[index], {
      enabled: structuredIngestionEnabled(newRows[index]),
      dropUnexpectedValues,
    });
    setRows(newRows);
  };

  const addTableColumns = useCallback(
    (table: string) => {
      const [schemaName, tableName] = table.split('.');

      startColumnsTransition(async () => {
        const res = await fetchColumns(sourcePeer, schemaName, tableName);
        setTableColumns((prev) => [
          ...prev,
          { tableName: table, columns: res },
        ]);
      });
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

  const fetchTablesForSchema = useCallback(
    (schemaName: string) => {
      startTablesTransition(async () => {
        try {
          const { tables: newRows, structuredIngestionSupported: supported } =
            await fetchTables(
              sourcePeer,
              schemaName,
              defaultTargetSchema,
              peerType,
              initialLoadOnly
            );
          setStructuredIngestionSupported(supported);

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
                row.structuredIngestionConfig =
                  existingRow.structuredIngestionConfig;
                // For a structured mapping the columns are the destination schema, and a
                // schemaless source reports none to rediscover, so they come from the
                // saved mapping or not at all.
                if (structuredIngestionEnabled(existingRow)) {
                  row.columns = existingRow.columns;
                }
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

          setFetchedSchemas((prev) => new Set(prev).add(schemaName));
        } catch (error) {
          // Handle error if needed
          console.error('Error fetching tables:', error);
        }
      });
    },
    [
      sourcePeer,
      defaultTargetSchema,
      peerType,
      alreadySelectedTables,
      addTableColumns,
      initialLoadOnly,
      setRows,
      setStructuredIngestionSupported,
    ]
  );

  const handleSchemaClick = (schemaName: string) => {
    if (!schemaIsExpanded(schemaName)) {
      setExpandedSchemas((curr) => [...curr, schemaName]);
      if (!fetchedSchemas.has(schemaName)) {
        fetchTablesForSchema(schemaName);
      }
    } else {
      setExpandedSchemas((curr) =>
        curr.filter((expandedSchema) => expandedSchema != schemaName)
      );
    }
  };

  const engineOptions = [
    { value: 'CH_ENGINE_REPLACING_MERGE_TREE', label: 'ReplacingMergeTree' },
    { value: 'CH_ENGINE_MERGE_TREE', label: 'MergeTree' },
    { value: 'CH_ENGINE_COALESCING_MERGE_TREE', label: 'CoalescingMergeTree' },
    { value: 'CH_ENGINE_NULL', label: 'Null' },
  ];

  return (
    <div style={schemaBoxStyle(styledTheme)}>
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
                setTargetSchemaOverride={applyTargetSchemaOverride}
              />
            </div>
          </div>
        </div>
        {/* TABLE BOX */}
        {schemaIsExpanded(schema) && (
          <div style={{ marginLeft: '1.25rem', marginTop: '0.75rem' }}>
            {searchedTables.length ? (
              searchedTables.map((row) => {
                const columns = getTableColumns(row.source);
                return (
                  <div key={row.source} style={tableBoxStyle(styledTheme)}>
                    <div
                      style={{
                        marginLeft: '1.25rem',
                        display: 'flex',
                        flexDirection: 'column',
                        rowGap: '1rem',
                      }}
                    >
                      <RowWithCheckbox
                        label={
                          <Tooltip
                            style={{
                              ...tooltipStyle(styledTheme),
                              display: row.canMirror ? 'none' : 'block',
                            }}
                            content={cannotMirrorReason(row)}
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
                                theme={selectTheme}
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
                      <div
                        style={{
                          marginLeft: '1.25rem',
                          marginTop: '0.75rem',
                          width: '100%',
                        }}
                      >
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
                                  {structuredIngestionSupported && (
                                    <div
                                      style={{ width: '100%', fontSize: 12 }}
                                    >
                                      <RowWithCheckbox
                                        label={
                                          <Label
                                            as='label'
                                            style={{ fontSize: 13 }}
                                          >
                                            <Tooltip
                                              style={tooltipStyle(styledTheme)}
                                              content='Project each document onto the columns declared below, using their destination type, instead of landing it whole in a single JSON column.'
                                            >
                                              Structured ingestion
                                            </Tooltip>
                                          </Label>
                                        }
                                        action={
                                          <Checkbox
                                            style={{ marginLeft: 0 }}
                                            disabled={row.editingDisabled}
                                            checked={structuredIngestionEnabled(
                                              row
                                            )}
                                            onCheckedChange={(state: boolean) =>
                                              updateStructuredIngestion(
                                                row.source,
                                                state
                                              )
                                            }
                                          />
                                        }
                                      />
                                      {structuredIngestionEnabled(row) && (
                                        <RowWithCheckbox
                                          label={
                                            <Label
                                              as='label'
                                              style={{ fontSize: 13 }}
                                            >
                                              <Tooltip
                                                style={tooltipStyle(
                                                  styledTheme
                                                )}
                                                content='Report values that do not fit their destination type without including the values themselves, only the field and the reason.'
                                              >
                                                Drop unexpected values from
                                                reports
                                              </Tooltip>
                                            </Label>
                                          }
                                          action={
                                            <Checkbox
                                              style={{ marginLeft: 0 }}
                                              disabled={row.editingDisabled}
                                              checked={
                                                structuredIngestionConfig(row)
                                                  ?.dropUnexpectedValues ??
                                                false
                                              }
                                              onCheckedChange={(
                                                state: boolean
                                              ) =>
                                                updateDropUnexpectedValues(
                                                  row.source,
                                                  state
                                                )
                                              }
                                            />
                                          }
                                        />
                                      )}
                                    </div>
                                  )}
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
                        {structuredIngestionEnabled(row) && (
                          <StructuredColumns
                            tableRow={row}
                            setRows={setRows}
                            disabled={row.editingDisabled}
                          />
                        )}
                      </div>
                    )}
                  </div>
                );
              })
            ) : tablesLoading ? (
              <div style={{ padding: '0.5rem 0', width: '40%' }}>
                <BarLoader width='100%' />
              </div>
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
