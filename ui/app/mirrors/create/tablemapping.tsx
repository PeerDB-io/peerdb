'use client';
import { DBType } from '@/grpc_generated/peers';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { SearchField } from '@/lib/SearchField';
import { TextField } from '@/lib/TextField';
import {
  Dispatch,
  SetStateAction,
  useCallback,
  useEffect,
  useState,
} from 'react';
import { BarLoader } from 'react-spinners/';
import { TableMapRow } from '../../dto/MirrorsDTO';
import { fetchColumns, fetchSchemas, fetchTables } from './handlers';
import { expandableStyle, schemaBoxStyle, tableBoxStyle } from './styles';

interface TableMappingProps {
  sourcePeerName: string;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  peerType?: DBType;
}

const TableMapping = ({
  sourcePeerName,
  rows,
  setRows,
  peerType,
}: TableMappingProps) => {
  const [allSchemas, setAllSchemas] = useState<string[]>();
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: string[] }[]
  >([]);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedSchemas, setExpandedSchemas] = useState<string[]>([]);

  const schemaIsExpanded = useCallback(
    (schema: string) => {
      return !!expandedSchemas.find((schemaName) => schemaName === schema);
    },
    [expandedSchemas]
  );

  const addTableColumns = (table: string) => {
    const schemaName = table.split('.')[0];
    const tableName = table.split('.')[1];
    fetchColumns(sourcePeerName, schemaName, tableName, setColumnsLoading).then(
      (res) =>
        setTableColumns((prev) => {
          return [...prev, { tableName: table, columns: res }];
        })
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

  const handleColumnExclusion = (
    source: string,
    column: string,
    include: boolean
  ) => {
    const currRows = [...rows];
    const rowWeNeed = currRows.find((row) => row.source === source);
    if (rowWeNeed) {
      const { exclude } = rowWeNeed;
      if (include) {
        const updatedExclude = exclude.filter((col) => col !== column);
        rowWeNeed.exclude = updatedExclude;
      } else {
        rowWeNeed.exclude.push(column);
      }
    }
    setRows(currRows);
  };

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

  const handleSchemaClick = (schemaName: string) => {
    if (!schemaIsExpanded(schemaName)) {
      setTablesLoading(true);
      setExpandedSchemas((curr) => [...curr, schemaName]);
      fetchTables(sourcePeerName, schemaName, peerType).then((tableRows) => {
        const newRows = [...rows, ...tableRows];
        setRows(newRows);
        setTablesLoading(false);
      });
    } else {
      setExpandedSchemas((curr) =>
        curr.filter((expandedSchema) => expandedSchema != schemaName)
      );
      setRows((curr) => curr.filter((row) => row.schema !== schemaName));
    }
  };

  useEffect(() => {
    fetchSchemas(sourcePeerName, setTablesLoading).then((res) =>
      setAllSchemas(res)
    );
  }, [sourcePeerName]);

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label as='label' colorName='lowContrast' style={{ fontSize: 14 }}>
        Select tables to sync
      </Label>
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
            placeholder='Search for schema'
            value={searchQuery}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(e.target.value)
            }
          />
        </div>
      </div>
      <div style={{ maxHeight: '40vh', overflow: 'scroll' }}>
        {allSchemas ? (
          allSchemas
            ?.filter((schema) => {
              return schema.toLowerCase().includes(searchQuery.toLowerCase());
            })
            .map((schema, index) => (
              <div key={index} style={schemaBoxStyle}>
                <div>
                  <div
                    style={{ ...expandableStyle, whiteSpace: 'nowrap' }}
                    onClick={() => handleSchemaClick(schema)}
                  >
                    <Icon
                      name={
                        schemaIsExpanded(schema)
                          ? 'arrow_drop_down'
                          : 'arrow_right'
                      }
                    />
                    <p>{schema}</p>
                  </div>
                  {schemaIsExpanded(schema) && (
                    <div className='ml-5 mt-3' style={{ width: '90%' }}>
                      {rows.filter((row) => row.schema === schema).length ? (
                        rows
                          .filter((row) => row.schema === schema)
                          .map((row, index) => {
                            const columns = getTableColumns(row.source);
                            return (
                              <div key={index} style={tableBoxStyle}>
                                <div
                                  style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                  }}
                                >
                                  <RowWithCheckbox
                                    label={
                                      <Label
                                        as='label'
                                        style={{ fontSize: 13 }}
                                      >
                                        {row.source}
                                      </Label>
                                    }
                                    action={
                                      <Checkbox
                                        onCheckedChange={(state: boolean) =>
                                          handleTableSelect(state, row.source)
                                        }
                                      />
                                    }
                                  />

                                  <div style={{ width: '40%' }}>
                                    <p style={{ fontSize: 12 }}>
                                      Target Table:
                                    </p>
                                    <TextField
                                      style={{
                                        fontSize: 12,
                                        marginTop: '0.5rem',
                                      }}
                                      variant='simple'
                                      placeholder={'Enter target table'}
                                      defaultValue={row.destination}
                                      onChange={(
                                        e: React.ChangeEvent<HTMLInputElement>
                                      ) =>
                                        updateDestination(
                                          row.source,
                                          e.target.value
                                        )
                                      }
                                    />
                                  </div>
                                </div>
                                {row.selected && (
                                  <div
                                    className='ml-5'
                                    style={{ width: '100%' }}
                                  >
                                    <Label
                                      as='label'
                                      colorName='lowContrast'
                                      style={{ fontSize: 13 }}
                                    >
                                      Columns
                                    </Label>
                                    {columns ? (
                                      columns.map((column, index) => {
                                        const columnName = column.split(':')[0];
                                        const columnType = column.split(':')[1];
                                        return (
                                          <RowWithCheckbox
                                            key={index}
                                            label={
                                              <Label
                                                as='label'
                                                style={{
                                                  fontSize: 13,
                                                  display: 'flex',
                                                }}
                                              >
                                                {columnName}{' '}
                                                <p
                                                  style={{
                                                    marginLeft: '0.5rem',
                                                    color: 'gray',
                                                  }}
                                                >
                                                  {columnType}
                                                </p>
                                              </Label>
                                            }
                                            action={
                                              <Checkbox
                                                checked={
                                                  !row.exclude.find(
                                                    (col) => col == columnName
                                                  )
                                                }
                                                onCheckedChange={(
                                                  state: boolean
                                                ) =>
                                                  handleColumnExclusion(
                                                    row.source,
                                                    columnName,
                                                    state
                                                  )
                                                }
                                              />
                                            }
                                          />
                                        );
                                      })
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
            ))
        ) : (
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              height: '100%',
            }}
          >
            <BarLoader color='#36d7b7' width='40%' />
          </div>
        )}
      </div>
    </div>
  );
};

export default TableMapping;
