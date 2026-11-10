'use client';
import { TableMapping } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { ColumnsItem } from '@/grpc_generated/route';
import { BarLoader } from '@/lib/BarLoader';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import Link from 'next/link';
import { Dispatch, SetStateAction, useEffect, useMemo, useState } from 'react';
import { useTheme } from 'styled-components';
import { TableMapRow } from '../../../dto/MirrorsDTO';
import { fetchSchemas } from '../handlers';
import EventhubsCallout from './eventhubsCallout';
import SchemaBox from './schemabox';
import { loaderContainer } from './styles';

interface TableMappingProps {
  sourcePeerName: string;
  sourceType?: DBType;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  peerType?: DBType;
  // schema -> omitted source table mapping
  alreadySelectedTablesMapping: Map<string, TableMapping[]>;
  initialLoadOnly: boolean;
  // Redesigned look, only used by the create-mirror page.
  redesign?: boolean;
}

export default function TablePicker({
  sourcePeerName,
  sourceType,
  rows,
  setRows,
  peerType,
  alreadySelectedTablesMapping,
  initialLoadOnly,
  redesign,
}: TableMappingProps) {
  const [allSchemas, setAllSchemas] = useState<string[]>();
  const [serverCounts, setServerCounts] = useState<Record<string, number>>();
  const [schemaQuery, setSchemaQuery] = useState('');
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: ColumnsItem[] }[]
  >([]);
  // Whether the source peer supports structured ingestion, as reported with its tables
  const [structuredIngestionSupported, setStructuredIngestionSupported] =
    useState(false);
  const searchedSchemas = useMemo(
    () =>
      allSchemas?.filter((schema) =>
        schema.toLowerCase().includes(schemaQuery.toLowerCase())
      ),
    [allSchemas, schemaQuery]
  );

  useEffect(() => {
    fetchSchemas(sourcePeerName).then((res) => {
      setAllSchemas(res.schemas);
      setServerCounts(res.tableCounts);
    });
  }, [sourcePeerName, initialLoadOnly]);

  // Redesign: the schema open in the tables pane.
  const theme = useTheme();
  const [activeSchema, setActiveSchema] = useState<string>();
  // Table count per schema: from the source when it reports them (Postgres),
  // otherwise known once the schema has been opened.
  const tableCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of rows) counts[row.schema] = (counts[row.schema] ?? 0) + 1;
    return { ...counts, ...serverCounts };
  }, [rows, serverCounts]);
  const selectedBySchema = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of rows) {
      if (row.selected) counts[row.schema] = (counts[row.schema] ?? 0) + 1;
    }
    return counts;
  }, [rows]);
  const blockedSchemas = useMemo(
    () => new Set(rows.filter((row) => !row.canMirror).map((row) => row.schema)),
    [rows]
  );
  useEffect(() => {
    if (!redesign || activeSchema || !allSchemas?.length) return;
    setActiveSchema(allSchemas[0]);
  }, [redesign, activeSchema, allSchemas]);

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label
        as='label'
        colorName={redesign ? undefined : 'lowContrast'}
        style={
          redesign ? { fontSize: 15, fontWeight: 600 } : { fontSize: 16 }
        }
      >
        {redesign ? 'Tables' : 'Select tables to sync'}
      </Label>
      <br />
      <Label as='label' style={{ fontSize: redesign ? 13.5 : 15 }}>
        Before selecting tables, please make sure that{' '}
        <Link
          style={{ color: redesign ? 'inherit' : 'teal', textDecoration: 'underline' }}
          target='_blank'
          href={
            'https://docs.peerdb.io/connect/postgres/rds_postgres#creating-peerdb-user-and-granting-permissions'
          }
        >
          these permissions
        </Link>{' '}
        have been granted for your tables.
      </Label>
      {peerType === DBType.EVENTHUBS && <EventhubsCallout />}
      {redesign ? (
        <div
          style={{
            display: 'grid',
            // schema list as wide as its longest name, the rest goes to the tables
            gridTemplateColumns: 'minmax(170px, max-content) minmax(0, 1fr)',
            height: 760,
            marginTop: '1rem',
            border: `1px solid ${theme.colors.base.border.subtle}`,
            borderRadius: '0.75rem',
            overflow: 'hidden',
            background: theme.colors.base.background.normal,
          }}
        >
          <nav
            aria-label='Schemas'
            style={{
              display: 'flex',
              flexDirection: 'column',
              minHeight: 0,
              borderRight: `1px solid ${theme.colors.base.border.subtle}`,
              background: theme.colors.base.background.subtle,
            }}
          >
            <div
              style={{
                padding: 12,
                borderBottom: `1px solid ${theme.colors.base.border.subtle}`,
              }}
            >
              <SearchField
                style={{ fontSize: 13 }}
                size={1}
                placeholder='Search schemas'
                value={schemaQuery}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  setSchemaQuery(e.target.value)
                }
              />
            </div>
            <div style={{ overflowY: 'auto', minHeight: 0 }}>
              {!searchedSchemas ? (
                <div style={{ padding: 14 }}>
                  <BarLoader width='60%' />
                </div>
              ) : (
                (() => {
                  const withSelection = searchedSchemas.filter(
                    (schema) => (selectedBySchema[schema] ?? 0) > 0
                  );
                  const empty = searchedSchemas.filter(
                    (schema) =>
                      tableCounts[schema] === 0 && !withSelection.includes(schema)
                  );
                  const rest = searchedSchemas.filter(
                    (schema) =>
                      !withSelection.includes(schema) && !empty.includes(schema)
                  );
                  const item = (schema: string) => {
                    const selected = selectedBySchema[schema] ?? 0;
                    const count = tableCounts?.[schema];
                    const isActive = schema === activeSchema;
                    return (
                      <button
                        key={schema}
                        type='button'
                        aria-current={isActive ? 'true' : undefined}
                        onClick={() => setActiveSchema(schema)}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: 8,
                          width: '100%',
                          height: 36,
                          padding: '0 14px',
                          border: 0,
                          cursor: 'pointer',
                          textAlign: 'left',
                          fontSize: 13.5,
                          fontWeight: isActive ? 600 : 400,
                          background: isActive
                            ? theme.colors.accent.surface.normal
                            : 'transparent',
                          boxShadow: isActive
                            ? `inset 0 0 0 1px ${theme.colors.accent.border.subtle}`
                            : 'none',
                          color: isActive
                            ? theme.colors.accent.text.lowContrast
                            : count === 0
                              ? theme.colors.base.fill.normal
                              : theme.colors.base.text.highContrast,
                        }}
                      >
                        {schema}
                        {selected > 0 && (
                          <span
                            style={{
                              fontSize: 11.5,
                              fontWeight: 600,
                              padding: '1px 7px',
                              borderRadius: 99,
                              background: theme.colors.accent.surface.normal,
                              color: theme.colors.accent.text.lowContrast,
                            }}
                          >
                            {selected}
                          </span>
                        )}
                        {blockedSchemas.has(schema) && (
                          <span
                            title='Has tables that cannot be replicated'
                            style={{
                              width: 6,
                              height: 6,
                              borderRadius: '50%',
                              background: theme.colors.warning.fill.normal,
                            }}
                          />
                        )}
                        <span
                          style={{
                            marginLeft: 'auto',
                            paddingLeft: 16,
                            fontSize: 12.5,
                            color: theme.colors.base.text.lowContrast,
                          }}
                        >
                          {count ?? ''}
                        </span>
                      </button>
                    );
                  };
                  const heading = (label: string) => (
                    <div
                      style={{
                        padding: '14px 14px 6px',
                        fontSize: 12,
                        fontWeight: 600,
                        color: theme.colors.base.text.lowContrast,
                      }}
                    >
                      {label}
                    </div>
                  );
                  return (
                    <>
                      {withSelection.length > 0 && heading('With selection')}
                      {withSelection.map(item)}
                      {heading('All schemas')}
                      {rest.map(item)}
                      {empty.length > 0 && heading('Empty')}
                      {empty.map(item)}
                    </>
                  );
                })()
              )}
            </div>
          </nav>
          <div style={{ overflowY: 'auto', minWidth: 0 }}>
            {searchedSchemas &&
              searchedSchemas.map((schema) => (
            <SchemaBox
              key={schema}
              schema={schema}
              sourcePeer={sourcePeerName}
              sourceType={sourceType}
              rows={rows}
              setRows={setRows}
              tableColumns={tableColumns}
              setTableColumns={setTableColumns}
              structuredIngestionSupported={structuredIngestionSupported}
              setStructuredIngestionSupported={setStructuredIngestionSupported}
              peerType={peerType}
              alreadySelectedTables={alreadySelectedTablesMapping.get(schema)}
              initialLoadOnly={initialLoadOnly}
              redesign={redesign}
              active={schema === activeSchema}
            />
          ))}
          </div>
        </div>
      ) : (
        <>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          marginTop: '0.5rem',
          padding: '0.5rem',
        }}
      >
        <div style={{ width: '30%' }}>
          <SearchField
            style={{ fontSize: 13 }}
            placeholder='Search for schemas'
            value={schemaQuery}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSchemaQuery(e.target.value)
            }
          />
        </div>
      </div>
      <div>
        {searchedSchemas ? (
          searchedSchemas.map((schema) => (
            <SchemaBox
              key={schema}
              schema={schema}
              sourcePeer={sourcePeerName}
              sourceType={sourceType}
              rows={rows}
              setRows={setRows}
              tableColumns={tableColumns}
              setTableColumns={setTableColumns}
              structuredIngestionSupported={structuredIngestionSupported}
              setStructuredIngestionSupported={setStructuredIngestionSupported}
              peerType={peerType}
              alreadySelectedTables={alreadySelectedTablesMapping.get(schema)}
              initialLoadOnly={initialLoadOnly}
              redesign={redesign}
              active={schema === activeSchema}
            />
          ))
        ) : (
          <div style={loaderContainer}>
            <BarLoader width='40%' />
          </div>
        )}
      </div>
        </>
      )}
    </div>
  );
}
