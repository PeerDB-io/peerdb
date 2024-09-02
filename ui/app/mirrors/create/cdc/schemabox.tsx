'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { DBType } from '@/grpc_generated/peers';
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
import { BarLoader } from 'react-spinners/';
import { fetchColumns, fetchTables } from '../handlers';
import ColumnBox from './columnbox';
import { SchemaSettings } from './schemasettings';
import {
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
  tableColumns: { tableName: string; columns: string[] }[];
  setTableColumns: Dispatch<
    SetStateAction<{ tableName: string; columns: string[] }[]>
  >;
  peerType?: DBType;
  omitAdditionalTables: string[] | undefined;
  initialLoadOnly?: boolean;
}

const SchemaBox = ({
  sourcePeer,
  peerType,
  schema,
  rows,
  setRows,
  tableColumns,
  setTableColumns,
  omitAdditionalTables,
  initialLoadOnly,
}: SchemaBoxProps) => {
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
      return !!expandedSchemas.find((schemaName) => schemaName === schema);
    },
    [expandedSchemas]
  );

  const handleAddRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index] = { ...newRows[index], selected: true };
    setRows(newRows);
    addTableColumns(source);
  };

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

  const updateDestination = (source: string, dest: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index] = { ...newRows[index], destination: dest };
    setRows(newRows);
  };

  const addTableColumns = (table: string) => {
    const schemaName = table.split('.')[0];
    const tableName = table.split('.')[1];

    fetchColumns(sourcePeer, schemaName, tableName, setColumnsLoading).then(
      (res) => {
        setTableColumns((prev) => {
          return [...prev, { tableName: table, columns: res }];
        });
      }
    );
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

  const rowsDoNotHaveSchemaTables = (schema: string) => {
    return !rows.some((row) => row.schema === schema);
  };

  const handleSchemaClick = (schemaName: string) => {
    if (!schemaIsExpanded(schemaName)) {
      setExpandedSchemas((curr) => [...curr, schemaName]);

      if (rowsDoNotHaveSchemaTables(schemaName)) {
        fetchTablesForSchema(schemaName);
      }
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
        for (const row of newRows) {
          if (omitAdditionalTables?.includes(row.source)) {
            row.canMirror = false;
          }
        }
        setRows((oldRows) => {
          const filteredRows = oldRows.filter(
            (oldRow) => oldRow.schema !== schemaName
          );
          const updatedRows = [...filteredRows, ...newRows];
          return updatedRows;
        });
        setTablesLoading(false);
      });
    },
    [
      setRows,
      sourcePeer,
      defaultTargetSchema,
      peerType,
      omitAdditionalTables,
      initialLoadOnly,
    ]
  );

  useEffect(() => {
    fetchTablesForSchema(schema);
  }, [schema, fetchTablesForSchema, initialLoadOnly]);

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
          <div className='ml-5 mt-3' style={{ width: '90%' }}>
            {searchedTables.length ? (
              searchedTables.map((row) => {
                const columns = getTableColumns(row.source);
                return (
                  <div key={row.source} style={tableBoxStyle}>
                    <div
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
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
                                color: row.canMirror ? 'black' : 'gray',
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
                          width: '40%',
                          display: row.selected ? 'block' : 'none',
                        }}
                        key={row.source}
                      >
                        <p style={{ fontSize: 12 }}>Target Table:</p>
                        <TextField
                          key={row.source}
                          style={{
                            fontSize: 12,
                            marginTop: '0.5rem',
                            cursor: 'pointer',
                          }}
                          variant='simple'
                          placeholder={'Enter target table'}
                          value={row.destination}
                          onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                            updateDestination(row.source, e.target.value)
                          }
                        />
                      </div>
                    </div>

                    {/* COLUMN BOX */}
                    {row.selected && (
                      <div className='ml-5' style={{ width: '100%' }}>
                        <Label
                          as='label'
                          colorName='lowContrast'
                          style={{ fontSize: 13 }}
                        >
                          Columns
                        </Label>
                        {columns ? (
                          <ColumnBox
                            columns={columns}
                            tableRow={row}
                            rows={rows}
                            setRows={setRows}
                          />
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
};

export default SchemaBox;
