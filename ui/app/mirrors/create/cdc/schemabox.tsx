'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { useSelectTheme } from '@/app/styles/select';
import {
  TableEngine,
  tableEngineFromJSON,
  tableEngineToJSON,
  TableMapping,
} from '@/grpc_generated/flow';
import { DBType, dBTypeFromJSON } from '@/grpc_generated/peers';
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
  useEffect,
  useMemo,
  useState,
  useTransition,
} from 'react';
import ReactSelect from 'react-select';
import { useTheme as useStyledTheme } from 'styled-components';
import {
  countTableRows,
  fetchColumns,
  fetchTables,
  getDefaultDestinationTable,
} from '../handlers';
import { notifyErr } from '@/app/utils/notify';
import {
  deletePreimageEnabled,
  isMongoSource,
  withDeletePreimage,
} from '../helpers/mongo';
import {
  structuredIngestionConfig,
  structuredIngestionEnabled,
  withStructuredIngestionConfig,
} from '../helpers/structured';
import ColumnBox from './columnbox';
import { cutTooltip } from './cutTooltip';
import CustomColumnType from './customColumnType';
import SchemaSettings from './schemasettings';
import SelectSortingKeys from './sortingkey';
import TableSettingsTabs from './tablesettings';
import StructuredColumns from './structuredColumns';
import {
  columnBoxDividerStyle,
  engineOptionStyles,
  expandableStyle,
  redesignTableBoxStyle,
  schemaBoxStyle,
  tableBoxStyle,
  tooltipStyle,
} from './styles';
// pg_size_pretty style sizes ("80 kB", "13 MB") to bytes, for schema totals.
function parseTableSize(size: string): number {
  const match = /^([\d.]+)\s*(bytes|kB|MB|GB|TB)$/.exec(size?.trim() ?? '');
  if (!match) return 0;
  const units: Record<string, number> = {
    bytes: 1,
    kB: 1e3,
    MB: 1e6,
    GB: 1e9,
    TB: 1e12,
  };
  return parseFloat(match[1]) * units[match[2]];
}

function formatTableSize(bytes: number): string {
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(bytes / 1e3))} kB`;
}

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
  sourceType?: DBType;
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
  // Redesigned look, only used by the create-mirror page.
  redesign?: boolean;
  // Redesign: the schema shown in the tables pane (others stay mounted, hidden).
  active?: boolean;
}

export default function SchemaBox({
  sourcePeer,
  sourceType,
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
  redesign,
  active,
}: SchemaBoxProps) {
  const selectTheme = useSelectTheme();
  // MongoDB specific settings apply to any MongoDB source, whether or not structured ingestion is on
  const mongoSource = isMongoSource(sourceType);
  const styledTheme = useStyledTheme();
  const [tablesLoading, startTablesTransition] = useTransition();
  const [columnsLoading, startColumnsTransition] = useTransition();
  const [expandedSchemas, setExpandedSchemas] = useState<string[]>([]);
  // tables whose exact count(*) is running
  const [counting, setCounting] = useState<Set<string>>(new Set());
  const canCount =
    sourceType !== undefined && dBTypeFromJSON(sourceType) === DBType.POSTGRES;
  const countExactly = async (sources: string[]) => {
    if (!sources.length) return;
    setCounting((prev) => new Set([...prev, ...sources]));
    try {
      const counts = await countTableRows(
        sourcePeer,
        schema,
        sources.map((source) => source.slice(schema.length + 1))
      );
      setRows((prev) =>
        prev.map((r) => {
          const n = counts[r.source.slice(schema.length + 1)];
          return r.schema === schema && n !== undefined
            ? { ...r, estimatedRows: n, rowCountKind: 'exact' }
            : r;
        })
      );
    } catch (e) {
      notifyErr(`Could not count rows: ${(e as Error).message}`);
    } finally {
      setCounting((prev) => {
        const next = new Set(prev);
        sources.forEach((source) => next.delete(source));
        return next;
      });
    }
  };
  // redesign: which selected tables show their settings panel
  const [openTables, setOpenTables] = useState<Set<string>>(new Set());
  const toggleOpen = (source: string, open: boolean) =>
    setOpenTables((prev) => {
      const next = new Set(prev);
      if (open) next.add(source);
      else next.delete(source);
      return next;
    });
  const [fetchedSchemas, setFetchedSchemas] = useState<Set<string>>(new Set());
  const [tableQuery, setTableQuery] = useState<string>('');
  const [tableFilter, setTableFilter] = useState<
    'all' | 'selected' | 'blocked'
  >('all');
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
          row.source.toLowerCase().includes(tableQueryLower) &&
          (tableFilter === 'all' ||
            (tableFilter === 'selected' && row.selected) ||
            (tableFilter === 'blocked' && !row.canMirror))
      )
      .sort((a, b) => a.source.localeCompare(b.source));
  }, [schema, rows, tableQuery, tableFilter]);

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

  const updateDeletePreimage = (source: string, enabled: boolean) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = withDeletePreimage(newRows[index], enabled);
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
    (schemaName: string, selectAll: boolean = false) => {
      startTablesTransition(async () => {
        try {
          const { tables: newRows, structuredIngestionSupported: supported } =
            await fetchTables(
              sourcePeer,
              schemaName,
              defaultTargetSchema,
              peerType,
              initialLoadOnly,
              peerType?.toString() === DBType[DBType.CLICKHOUSE].toString()
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
                row.mongoConfig = existingRow.mongoConfig;
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

          // Checking a schema before its tables were loaded selects them on arrival
          if (selectAll) {
            for (const row of newRows) {
              if (row.canMirror && !row.editingDisabled) {
                row.selected = true;
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
      redesign,
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

  // Redesign: the active schema is always open, its tables loaded on first view.
  useEffect(() => {
    if (!redesign || !active) return;
    if (!expandedSchemas.includes(schema)) {
      setExpandedSchemas((curr) => [...curr, schema]);
    }
    if (!fetchedSchemas.has(schema)) {
      fetchTablesForSchema(schema);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [redesign, active, schema]);

  const engineOptions = [
    { value: 'CH_ENGINE_REPLACING_MERGE_TREE', label: 'ReplacingMergeTree' },
    { value: 'CH_ENGINE_MERGE_TREE', label: 'MergeTree' },
    { value: 'CH_ENGINE_COALESCING_MERGE_TREE', label: 'CoalescingMergeTree' },
    { value: 'CH_ENGINE_NULL', label: 'Null' },
  ];

  const gridCols =
    '40px minmax(0, 1.3fr) minmax(0, 1.2fr) 64px 64px minmax(0, 0.8fr) 30px';
  const tipColors = {
    bg: styledTheme.colors.base.text.highContrast,
    fg: styledTheme.colors.base.background.normal,
  };
  const schemaRows = rows.filter((row) => row.schema === schema);
  const schemaSelected = schemaRows.filter((row) => row.selected).length;
  const schemaMirrorable = schemaRows.filter(
    (row) => row.canMirror && !row.editingDisabled
  );
  const schemaAllSelected =
    schemaMirrorable.length > 0 && schemaMirrorable.every((row) => row.selected);
  const schemaSomeSelected = schemaSelected > 0 && !schemaAllSelected;
  const schemaBlocked = schemaRows.filter((row) => !row.canMirror).length;
  const schemaBytes = schemaRows.reduce(
    (total, row) => total + parseTableSize(row.tableSize),
    0
  );
  // Selects or clears every replicable table of the schema, loading them first
  // when the schema was never opened.
  const toggleSchemaSelection = (checked: boolean) => {
    if (!fetchedSchemas.has(schema)) {
      if (!checked) return;
      if (!schemaIsExpanded(schema)) {
        setExpandedSchemas((curr) => [...curr, schema]);
      }
      fetchTablesForSchema(schema, true);
      return;
    }
    const newRows = rows.map((row) => {
      if (row.schema !== schema || !row.canMirror || row.editingDisabled) {
        return row;
      }
      if (checked) addTableColumns(row.source);
      else removeTableColumns(row.source);
      return { ...row, selected: checked };
    });
    setRows(newRows);
  };
  const keyTag = (row: TableMapRow, columns?: ColumnsItem[]) => {
    const ok = row.canMirror;
    if (ok && !row.isReplicaIdentityFull) {
      const keys = row.keyColumns?.length
        ? row.keyColumns
        : columns?.filter((c) => c.isKey).map((c) => c.name);
      return (
        <span
          style={{
            fontSize: 13,
            color: styledTheme.colors.base.text.lowContrast,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
          {...cutTooltip(keys?.join(', ') ?? '', tipColors)}
        >
          {keys?.length ? keys.join(', ') : 'Primary key'}
        </span>
      );
    }
    const tone = styledTheme.colors.warning;
    return (
      <span
        title={
          ok
            ? 'REPLICA IDENTITY FULL: updates are matched on every column, slower than a primary key'
            : cannotMirrorReason(row)
        }
        style={{
          justifySelf: 'start',
          fontSize: 11.5,
          fontWeight: 600,
          padding: '2px 8px',
          borderRadius: 99,
          whiteSpace: 'nowrap',
          background: tone.surface.normal,
          color: tone.text.lowContrast,
        }}
      >
        {ok ? 'Full row' : 'No key'}
      </span>
    );
  };
  const cellMuted = {
    fontSize: 13,
    color: styledTheme.colors.base.text.lowContrast,
  };

  return (
    <div
      style={
        redesign
          ? { display: active ? 'block' : 'none', minWidth: 0 }
          : schemaBoxStyle(styledTheme)
      }
    >
      <div
        style={
          redesign
            ? {
                padding: '0 1.25rem 0.75rem',
                borderBottom: `1px solid ${styledTheme.colors.base.border.subtle}`,
              }
            : undefined
        }
      >
        {redesign ? (
          <div
            style={{
              position: 'sticky',
              top: 0,
              zIndex: 3,
              margin: '0 -1.25rem',
              padding: '1rem 1.25rem 0',
              background: styledTheme.colors.base.background.normal,
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <h3
                style={{
                  margin: 0,
                  fontSize: 20,
                  fontWeight: 600,
                  letterSpacing: '-0.02em',
                  color: styledTheme.colors.base.text.highContrast,
                }}
              >
                {schema}
              </h3>
              {fetchedSchemas.has(schema) && (
                <span style={cellMuted}>
                  {schemaRows.length}{' '}
                  {schemaRows.length === 1 ? 'table' : 'tables'}
                  {schemaBytes > 0 ? `, ${formatTableSize(schemaBytes)}` : ''}
                </span>
              )}
              <span style={{ flexGrow: 1 }} />
              {canCount &&
                (() => {
                  const pending = schemaRows
                    .filter((r) => r.rowCountKind !== 'exact')
                    .map((r) => r.source);
                  if (!pending.length) return null;
                  const busy = pending.some((source) => counting.has(source));
                  return (
                    <button
                      type='button'
                      disabled={busy || tablesLoading}
                      onClick={() => countExactly(pending)}
                      title='Runs count(*) on these tables. Big tables take a while.'
                      style={{
                        height: 34,
                        padding: '0 14px',
                        borderRadius: 8,
                        cursor: busy ? 'default' : 'pointer',
                        fontSize: 13,
                        fontWeight: 500,
                        border: `1px solid ${styledTheme.colors.base.border.normal}`,
                        background: styledTheme.colors.base.background.normal,
                        color: styledTheme.colors.base.text.highContrast,
                      }}
                    >
                      {busy ? 'Counting…' : `Count ${pending.length} exactly`}
                    </button>
                  );
                })()}
              {schemaMirrorable.length > 0 && (
                <button
                  type='button'
                  disabled={tablesLoading}
                  onClick={() => toggleSchemaSelection(!schemaAllSelected)}
                  style={{
                    height: 34,
                    padding: '0 14px',
                    borderRadius: 8,
                    cursor: 'pointer',
                    fontSize: 13,
                    fontWeight: 500,
                    border: `1px solid ${styledTheme.colors.base.border.normal}`,
                    background: styledTheme.colors.base.background.normal,
                    color: styledTheme.colors.base.text.highContrast,
                  }}
                >
                  {schemaAllSelected
                    ? 'Clear selection'
                    : `Select all ${schemaMirrorable.length} replicable`}
                </button>
              )}
              <div style={{ alignSelf: 'center', cursor: 'pointer' }}>
                <SchemaSettings
                  schema={defaultTargetSchema}
                  setTargetSchemaOverride={applyTargetSchemaOverride}
                />
              </div>
            </div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                marginTop: 12,
              }}
            >
              <div style={{ width: 260 }}>
                <SearchField
                  style={{ fontSize: 13 }}
                  placeholder={`Search tables in ${schema}`}
                  value={tableQuery}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    setTableQuery(e.target.value)
                  }
                />
              </div>
              {(
                (key: 'all' | 'selected' | 'blocked', label: string) => (
                <button
                  key={key}
                  type='button'
                  aria-pressed={tableFilter === key}
                  onClick={() => setTableFilter(key)}
                  style={{
                    height: 28,
                    padding: '0 11px',
                    borderRadius: 99,
                    cursor: 'pointer',
                    fontSize: 12.5,
                    fontWeight: 500,
                    border: `1px solid ${
                      tableFilter === key
                        ? styledTheme.colors.base.text.highContrast
                        : styledTheme.colors.base.border.normal
                    }`,
                    background:
                      tableFilter === key
                        ? styledTheme.colors.base.text.highContrast
                        : styledTheme.colors.base.background.normal,
                    color:
                      tableFilter === key
                        ? styledTheme.colors.base.background.normal
                        : styledTheme.colors.base.text.highContrast,
                  }}
                >
                  {label}
                </button>
              )
              )('all', `All ${schemaRows.length}`)}
              {((key: 'all' | 'selected' | 'blocked', label: string) => (
                <button
                  key={key}
                  type='button'
                  aria-pressed={tableFilter === key}
                  onClick={() => setTableFilter(key)}
                  style={{
                    height: 28,
                    padding: '0 11px',
                    borderRadius: 99,
                    cursor: 'pointer',
                    fontSize: 12.5,
                    fontWeight: 500,
                    border: `1px solid ${
                      tableFilter === key
                        ? styledTheme.colors.base.text.highContrast
                        : styledTheme.colors.base.border.normal
                    }`,
                    background:
                      tableFilter === key
                        ? styledTheme.colors.base.text.highContrast
                        : styledTheme.colors.base.background.normal,
                    color:
                      tableFilter === key
                        ? styledTheme.colors.base.background.normal
                        : styledTheme.colors.base.text.highContrast,
                  }}
                >
                  {label}
                </button>
              ))('selected', `Selected ${schemaSelected}`)}
              {schemaBlocked > 0 &&
                ((key: 'all' | 'selected' | 'blocked', label: string) => (
                <button
                  key={key}
                  type='button'
                  aria-pressed={tableFilter === key}
                  onClick={() => setTableFilter(key)}
                  style={{
                    height: 28,
                    padding: '0 11px',
                    borderRadius: 99,
                    cursor: 'pointer',
                    fontSize: 12.5,
                    fontWeight: 500,
                    border: `1px solid ${
                      tableFilter === key
                        ? styledTheme.colors.base.text.highContrast
                        : styledTheme.colors.base.border.normal
                    }`,
                    background:
                      tableFilter === key
                        ? styledTheme.colors.base.text.highContrast
                        : styledTheme.colors.base.background.normal,
                    color:
                      tableFilter === key
                        ? styledTheme.colors.base.background.normal
                        : styledTheme.colors.base.text.highContrast,
                  }}
                >
                  {label}
                </button>
              ))('blocked', `Not replicable ${schemaBlocked}`)}
            </div>
            {schemaIsExpanded(schema) && searchedTables.length > 0 && (
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: gridCols,
                  columnGap: 12,
                  alignItems: 'center',
                  height: 36,
                  padding: '0 1rem',
                  fontSize: 12.5,
                  fontWeight: 500,
                  color: styledTheme.colors.base.text.lowContrast,
                  marginTop: 12,
                  borderTop: `1px solid ${styledTheme.colors.base.border.subtle}`,
                }}
              >
                <span />
                <span>Source table</span>
                <span>Target table</span>
                <span style={{ textAlign: 'right' }}>Rows</span>
                <span style={{ textAlign: 'right' }}>Size</span>
                <span>Key</span>
                <span />
              </div>
            )}
          </div>
        ) : (
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
        )}
        {/* TABLE BOX */}
        {schemaIsExpanded(schema) && (
          <div
            style={
              redesign
                ? undefined
                : { marginLeft: '1.25rem', marginTop: '0.75rem' }
            }
          >
            {searchedTables.length ? (
              searchedTables.map((row) => {
                const columns = getTableColumns(row.source);
                return (
                  <div
                    key={row.source}
                    style={
                      redesign
                        ? {
                            ...redesignTableBoxStyle(styledTheme),
                            padding:
                              row.selected && openTables.has(row.source)
                                ? '0 1rem 1.25rem'
                                : '0 1rem',
                          }
                        : tableBoxStyle(styledTheme)
                    }
                  >
                    <div
                      style={{
                        marginLeft: redesign ? 0 : '1.25rem',
                        display: 'flex',
                        flexDirection: 'column',
                        rowGap: '1rem',
                      }}
                    >
                      {redesign ? (
                        <div
                          style={{
                            display: 'grid',
                            gridTemplateColumns: gridCols,
                            columnGap: 12,
                            alignItems: 'center',
                            minHeight: 52,
                          }}
                        >
                          <Checkbox
                            disabled={!row.canMirror}
                            checked={row.selected}
                            onCheckedChange={(state: boolean) =>
                              handleTableSelect(state, row.source)
                            }
                          />
                          <Tooltip
                            style={{
                              ...tooltipStyle(styledTheme),
                              display: row.canMirror ? 'none' : 'block',
                            }}
                            content={cannotMirrorReason(row)}
                          >
                            <Label
                              as='label'
                              title={row.source}
                              style={{
                                display: 'block',
                                fontSize: 13,
                                padding: 0,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                color: row.canMirror ? undefined : 'gray',
                              }}
                              {...cutTooltip(row.source.slice(schema.length + 1), tipColors)}
                            >
                              {row.source.slice(schema.length + 1)}
                            </Label>
                          </Tooltip>
                          {row.selected ? (
                            <div>
                              <TextField
                                disabled={row.editingDisabled}
                                variant='simple'
                                style={{ fontSize: 13 }}
                                {...cutTooltip(row.destination, tipColors)}
                                placeholder='Enter target table'
                                value={row.destination}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) =>
                                  updateDestination(row.source, e.target.value)
                                }
                              />
                              {rows.some(
                                (other) =>
                                  other.source !== row.source &&
                                  other.selected &&
                                  other.destination === row.destination
                              ) && (
                                <div
                                  role='alert'
                                  style={{
                                    marginTop: 4,
                                    fontSize: 12,
                                    color:
                                      styledTheme.colors.destructive.text
                                        .lowContrast,
                                  }}
                                >
                                  Another selected table uses this name.
                                </div>
                              )}
                            </div>
                          ) : (
                            <span style={cellMuted}>
                              {row.canMirror
                                ? 'Not selected'
                                : 'Cannot be mirrored'}
                            </span>
                          )}
                          <button
                            type='button'
                            // not `disabled`: disabled buttons drop mouse events and the tooltip
                            aria-disabled={
                              !canCount ||
                              row.rowCountKind === 'exact' ||
                              counting.has(row.source)
                            }
                            onClick={() =>
                              canCount &&
                              row.rowCountKind !== 'exact' &&
                              !counting.has(row.source) &&
                              countExactly([row.source])
                            }
                            aria-label={`Count rows of ${row.source} exactly`}
                            style={{
                              ...cellMuted,
                              textAlign: 'right',
                              border: 0,
                              padding: 0,
                              background: 'none',
                              fontFamily: 'inherit',
                              cursor:
                                canCount && row.rowCountKind !== 'exact'
                                  ? 'pointer'
                                  : 'default',
                            }}
                            {...cutTooltip(
                              (counting.has(row.source)
                                ? 'Counting…'
                                : row.rowCountKind === 'exact'
                                  ? 'Exact count'
                                  : row.estimatedRows !== undefined
                                    ? 'Approximate, from Postgres statistics. Can be well off if the table was not analyzed recently'
                                    : "No estimate yet: Postgres hasn't analyzed this table. It replicates normally.") +
                                (canCount && row.rowCountKind !== 'exact' && !counting.has(row.source)
                                  ? ' · Click to count exactly'
                                  : ''),
                              tipColors,
                              true
                            )}
                          >
                            {counting.has(row.source)
                              ? 'Counting…'
                              : row.estimatedRows !== undefined
                                ? `${row.rowCountKind === 'exact' ? '' : '~'}${Number(row.estimatedRows).toLocaleString('en-US')}`
                                : '—'}
                          </button>
                          <span style={{ ...cellMuted, textAlign: 'right' }}>
                            {row.tableSize || '—'}
                          </span>
                          {keyTag(row, columns)}
                          {row.selected ? (
                            <button
                              type='button'
                              aria-expanded={openTables.has(row.source)}
                              aria-label={`${openTables.has(row.source) ? 'Hide' : 'Show'} settings for ${row.source}`}
                              title={openTables.has(row.source) ? 'Hide settings' : 'Show settings'}
                              onClick={() =>
                                toggleOpen(row.source, !openTables.has(row.source))
                              }
                              style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                width: 30,
                                height: 30,
                                padding: 0,
                                border: `1px solid ${styledTheme.colors.base.border.subtle}`,
                                borderRadius: 6,
                                background: 'none',
                                color: styledTheme.colors.base.text.highContrast,
                                cursor: 'pointer',
                              }}
                            >
                              <Icon
                                name={openTables.has(row.source) ? 'arrow_drop_up' : 'arrow_drop_down'}
                              />
                            </button>
                          ) : (
                            <span />
                          )}
                        </div>
                      ) : (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                      <button
                        type='button'
                        aria-expanded={openTables.has(row.source)}
                        aria-label={
                          openTables.has(row.source)
                            ? 'Hide settings'
                            : 'Show settings'
                        }
                        onClick={() =>
                          toggleOpen(row.source, !openTables.has(row.source))
                        }
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          width: 28,
                          height: 28,
                          padding: 0,
                          border: 0,
                          background: 'none',
                          color: styledTheme.colors.base.text.highContrast,
                          cursor: 'pointer',
                          flexShrink: 0,
                        }}
                      >
                        <Icon
                          name={
                            openTables.has(row.source)
                              ? 'arrow_drop_down'
                              : 'arrow_right'
                          }
                        />
                      </button>
                      <div style={{ flex: 1, minWidth: 0 }}>
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
                            {redesign && row.estimatedRows !== undefined && (
                              <Label
                                as='label'
                                colorName='lowContrast'
                                style={{ fontSize: 13 }}
                              >
                                ~{row.estimatedRows.toLocaleString('en-US')} rows
                              </Label>
                            )}
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
                      </div>
                      </div>
                      )}
                      <div
                        style={{
                          display: !redesign && openTables.has(row.source) ? 'grid' : 'none',
                          gridTemplateColumns:
                            'minmax(0, 1.6fr) minmax(180px, 0.7fr)',
                          gap: '14px 20px',
                          width: '100%',
                        }}
                        key={row.source}
                      >
                        {!redesign && (
                        <div style={{ width: '100%', minWidth: 0, fontSize: 12 }}>
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
                        )}

                        {peerType?.toString() ===
                          DBType[DBType.CLICKHOUSE].toString() && (
                          <div style={{ width: '100%', minWidth: 0, fontSize: 12 }}>
                            Engine:
                            <ReactSelect
                              isDisabled={row.editingDisabled}
                              styles={{
                                ...engineOptionStyles,
                                container: (base: any) => ({
                                  ...base,
                                  width: '100%',
                                  marginTop: '0.5rem',
                                }),
                                control: (base: any) => ({
                                  ...engineOptionStyles.control(base),
                                  width: '100%',
                                  minHeight: 28,
                                  height: 28,
                                }),
                                valueContainer: (base: any) => ({
                                  ...engineOptionStyles.valueContainer(base),
                                  height: 26,
                                }),
                                indicatorsContainer: (base: any) => ({
                                  ...base,
                                  height: 26,
                                }),
                              }}
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
                        )}
                      </div>
                    </div>

                    {/* COLUMN BOX */}
                    {((redesign && row.selected && openTables.has(row.source)) ||
                      (!redesign && openTables.has(row.source))) && (
                      <TableSettingsTabs
                        enabled={!!redesign}
                        isClickHouse={
                          peerType?.toString() ===
                          DBType[DBType.CLICKHOUSE].toString()
                        }
                        row={row}
                        columns={columns}
                        setRows={setRows}
                        engineOptions={engineOptions}
                        engineValue={
                          typeof row.engine === 'string'
                            ? row.engine
                            : tableEngineToJSON(row.engine)
                        }
                        onEngine={(v) =>
                          updateEngine(row.source, tableEngineFromJSON(v))
                        }
                        onPartitionKey={(v) => updatePartitionKey(row.source, v)}
                        onShardingKey={(v) => updateShardingKey(row.source, v)}
                        onPolicyName={(v) => updatePolicyName(row.source, v)}
                        onPartitionByExpr={(v) =>
                          updatePartitionByExpr(row.source, v)
                        }
                      >
                      <div
                        style={{
                          marginTop: redesign ? 0 : '0.75rem',
                          width: '100%',
                          minWidth: 0,
                        }}
                      >
                        {!redesign && (
                        <>
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
                            style={{
                              fontSize: 13,
                              padding: redesign ? 0 : undefined,
                            }}
                          >
                            Columns
                          </Label>
                        </div>
                        </>
                        )}
                        {columns ? (
                          <>
                            <div
                              style={{
                                display: 'grid',
                                gridTemplateColumns:
                                  'repeat(2, minmax(0, 1fr))',
                                alignItems: 'start',
                                columnGap: '2.5rem',
                              }}
                            >
                              <ColumnBox
                                compact={redesign}
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
                            </div>
                            {!redesign &&
                              peerType?.toString() !==
                                DBType[DBType.CLICKHOUSE].toString() && (
                                <div style={{ minWidth: 0, fontSize: 12, marginTop: '0.75rem' }}>
                                  Custom Partitioning Key:
                                  <TextField
                                    disabled={row.editingDisabled}
                                    style={{ marginTop: '0.5rem' }}
                                    variant='simple'
                                    placeholder='Enter optional custom partiton key'
                                    value={row.partitionKey}
                                    onChange={(
                                      e: React.ChangeEvent<HTMLInputElement>
                                    ) =>
                                      updatePartitionKey(row.source, e.target.value)
                                    }
                                  />
                                </div>
                              )}
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
                                {!redesign && (
                                  <>
                                    <hr style={columnBoxDividerStyle} />
                                    <Label as='label' style={{ fontSize: 13 }}>
                                      ClickHouse
                                    </Label>
                                    <div
                                      style={{
                                        display: 'grid',
                                        gridTemplateColumns:
                                          'repeat(4, minmax(0, 1fr))',
                                        gap: '14px 16px',
                                        width: '100%',
                                        minWidth: 0,
                                      }}
                                    >
                                      <div style={{ minWidth: 0, fontSize: 12 }}>
                                        Custom Partitioning Key:
                                        <TextField
                                          disabled={row.editingDisabled}
                                          style={{ marginTop: '0.5rem' }}
                                          variant='simple'
                                          placeholder='Enter optional custom partiton key'
                                          value={row.partitionKey}
                                          onChange={(
                                            e: React.ChangeEvent<HTMLInputElement>
                                          ) =>
                                            updatePartitionKey(
                                              row.source,
                                              e.target.value
                                            )
                                          }
                                        />
                                      </div>
                                      <div style={{ minWidth: 0, fontSize: 12 }}>
                                        Sharding Key:
                                        <TextField
                                          disabled={row.editingDisabled}
                                          style={{ marginTop: '0.5rem' }}
                                          variant='simple'
                                          placeholder='Sharding key expression (optional)'
                                          value={row.shardingKey}
                                          onChange={(
                                            e: React.ChangeEvent<HTMLInputElement>
                                          ) =>
                                            updateShardingKey(
                                              row.source,
                                              e.target.value
                                            )
                                          }
                                        />
                                      </div>
                                      <div style={{ minWidth: 0, fontSize: 12 }}>
                                        Policy Name:
                                        <TextField
                                          disabled={row.editingDisabled}
                                          style={{ marginTop: '0.5rem' }}
                                          variant='simple'
                                          placeholder='Policy name (optional)'
                                          value={row.policyName}
                                          onChange={(
                                            e: React.ChangeEvent<HTMLInputElement>
                                          ) =>
                                            updatePolicyName(
                                              row.source,
                                              e.target.value
                                            )
                                          }
                                        />
                                      </div>
                                      <div style={{ minWidth: 0, fontSize: 12 }}>
                                        Partition By Expr:
                                        <TextField
                                          disabled={row.editingDisabled}
                                          style={{ marginTop: '0.5rem' }}
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
                                    </div>
                                  </>
                                )}
                                <div style={{ width: '100%' }}>
                                  {!redesign && (
                                  <SelectSortingKeys
                                    columns={columns
                                      .map((column) => column.name)
                                      .filter((name) => !row.exclude.has(name))}
                                    loading={columnsLoading}
                                    tableRow={row}
                                    setRows={setRows}
                                  />
                                  )}
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
                                  {mongoSource && (
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
                                              content='Carry the document as it stood before the delete on delete events, instead of only its _id, so the deleted row keeps its values. Requires changeStreamPreAndPostImages to be enabled on the collection.'
                                            >
                                              Include pre-image on deletes
                                            </Tooltip>
                                          </Label>
                                        }
                                        action={
                                          <Checkbox
                                            style={{ marginLeft: 0 }}
                                            disabled={row.editingDisabled}
                                            checked={deletePreimageEnabled(row)}
                                            onCheckedChange={(state: boolean) =>
                                              updateDeletePreimage(
                                                row.source,
                                                state
                                              )
                                            }
                                          />
                                        }
                                      />
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
                      </TableSettingsTabs>
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
