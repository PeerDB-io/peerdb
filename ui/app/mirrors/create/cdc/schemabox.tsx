'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { DBType } from '@/grpc_generated/peers';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { SearchField } from '@/lib/SearchField';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction, useCallback, useState } from 'react';
import { BarLoader } from 'react-spinners/';
import { fetchColumns, fetchTables } from '../handlers';
import { expandableStyle, schemaBoxStyle, tableBoxStyle } from './styles';

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
}
const SchemaBox = ({
  sourcePeer,
  peerType,
  schema,
  rows,
  setRows,
  tableColumns,
  setTableColumns,
}: SchemaBoxProps) => {
  const [tablesLoading, setTablesLoading] = useState(false);
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [expandedSchemas, setExpandedSchemas] = useState<string[]>([]);
  const [tableQuery, setTableQuery] = useState<string>('');

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
    const rowOfSource = currRows.find((row) => row.source === source);
    if (rowOfSource) {
      if (include) {
        const updatedExclude = rowOfSource.exclude.filter(
          (col) => col !== column
        );
        rowOfSource.exclude = updatedExclude;
      } else {
        rowOfSource.exclude.push(column);
      }
    }
    setRows(currRows);
  };

  const handleSelectAll = (
    e: React.MouseEvent<HTMLInputElement, MouseEvent>
  ) => {
    const newRows = [...rows];
    for (const row of newRows) {
      row.selected = e.currentTarget.checked;
      if (e.currentTarget.checked) addTableColumns(row.source);
      else removeTableColumns(row.source);
    }
    setRows(newRows);
  };

  const handleSchemaClick = (schemaName: string) => {
    if (!schemaIsExpanded(schemaName)) {
      setTablesLoading(true);
      setExpandedSchemas((curr) => [...curr, schemaName]);
      fetchTables(sourcePeer, schemaName, peerType).then((tableRows) => {
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
              <input type='checkbox' onClick={(e) => handleSelectAll(e)} />
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
          </div>
        </div>
        {schemaIsExpanded(schema) && (
          <div className='ml-5 mt-3' style={{ width: '90%' }}>
            {rows.filter((row) => row.schema === schema).length ? (
              rows
                .filter(
                  (row) =>
                    row.schema === schema &&
                    row.source.toLowerCase().includes(tableQuery.toLowerCase())
                )
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
                            <Label as='label' style={{ fontSize: 13 }}>
                              {row.source}
                            </Label>
                          }
                          action={
                            <Checkbox
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
                            }}
                            variant='simple'
                            placeholder={'Enter target table'}
                            defaultValue={row.destination}
                            onChange={(
                              e: React.ChangeEvent<HTMLInputElement>
                            ) => updateDestination(row.source, e.target.value)}
                          />
                        </div>
                      </div>
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
                            columns.map((column, index) => {
                              const columnName = column.split(':')[0];
                              const columnType = column.split(':')[1];
                              const isPkey = column.split(':')[2] === 'true';
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
                                      disabled={isPkey}
                                      checked={
                                        !row.exclude.find(
                                          (col) => col == columnName
                                        )
                                      }
                                      onCheckedChange={(state: boolean) =>
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
  );
};

export default SchemaBox;
