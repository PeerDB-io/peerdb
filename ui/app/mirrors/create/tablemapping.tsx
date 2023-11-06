'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { SearchField } from '@/lib/SearchField';
import { Select, SelectItem } from '@/lib/Select';
import { Switch } from '@/lib/Switch';
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
import ColumnsDisplay from './columns';
import { fetchSchemas, fetchTables } from './handlers';

interface TableMappingProps {
  sourcePeerName: string;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  schema: string;
  setSchema: Dispatch<SetStateAction<string>>;
}

const TableMapping = ({
  sourcePeerName,
  rows,
  setRows,
  schema,
  setSchema,
}: TableMappingProps) => {
  const [allSchemas, setAllSchemas] = useState<string[]>();
  const [allTables, setAllTables] = useState<string[]>();
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: string[] }[]
  >([]);
  const [loading, setLoading] = useState(false);

  const handleAddRow = (source: string) => {
    setRows([...rows, { source, destination: source, partitionKey: '' }]);
  };

  const handleRemoveRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows.splice(index, 1);
    setRows(newRows);
  };

  const getRow = (source: string) => {
    const newRows = [...rows];
    const row = newRows.find((row) => row.source === `${schema}.${source}`);
    return row;
  };

  const handleSelectAll = (
    e: React.MouseEvent<HTMLInputElement, MouseEvent>
  ) => {
    if (e.currentTarget.checked) {
      const tableNames: TableMapRow[] | undefined = allTables?.map(
        (tableName) => {
          return {
            source: `${schema}.${tableName}`,
            destination: `${schema}.${tableName}`,
            partitionKey: '',
          };
        }
      );
      setRows(tableNames ?? []);
    } else setRows([]);
  };

  const handleSwitch = (on: boolean, source: string) => {
    if (on) {
      handleAddRow(`${schema}.${source}`);
    } else {
      handleRemoveRow(`${schema}.${source}`);
    }
  };

  const updateDestination = (source: string, dest: string) => {
    // find the row with source and update the destination
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index].destination = dest;
    return newRows;
  };

  const updatePartitionKey = (source: string, pkey: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    newRows[index].partitionKey = pkey;
    return newRows;
  };

  const getTablesOfSchema = useCallback(
    (schemaName: string) => {
      fetchTables(sourcePeerName, schemaName, setLoading).then((res) =>
        setAllTables(res)
      );
    },
    [sourcePeerName]
  );

  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (searchQuery.length > 0) {
      setAllTables(
        (curr) =>
          curr?.filter((table) => {
            return table.toLowerCase().includes(searchQuery.toLowerCase());
          })
      );
    }
    if (searchQuery.length == 0) {
      getTablesOfSchema(schema);
    }
  }, [searchQuery, getTablesOfSchema]);

  useEffect(() => {
    fetchSchemas(sourcePeerName, setLoading).then((res) => setAllSchemas(res));
    setSchema('public');
    getTablesOfSchema('public');
  }, [sourcePeerName, setSchema, getTablesOfSchema]);

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label colorName='lowContrast'>Select tables to sync</Label>
      <RowWithSelect
        label={<Label>Source Schema</Label>}
        action={
          <Select
            placeholder='Select a schema'
            onValueChange={(val: string) => {
              setSchema(val);
              getTablesOfSchema(val);
            }}
            value={schema.length > 0 ? schema : 'Loading...'}
          >
            {allSchemas ? (
              allSchemas.map((schemaName, id) => {
                return (
                  <SelectItem key={id} value={schemaName}>
                    {schemaName}
                  </SelectItem>
                );
              })
            ) : (
              <p>Loading schemas...</p>
            )}
          </Select>
        }
      />
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          marginTop: '0.5rem',
          padding: '0.5rem',
        }}
      >
        <div style={{ display: 'flex' }}>
          <input type='checkbox' onClick={(e) => handleSelectAll(e)} />
          <Label>Select All</Label>
        </div>
        <div style={{ width: '30%' }}>
          <SearchField
            placeholder='Search'
            value={searchQuery}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(e.target.value)
            }
          />
        </div>
      </div>
      <div style={{ maxHeight: '40vh', overflow: 'scroll' }}>
        {allTables ? (
          allTables.map((sourceTableName, index) => (
            <div
              key={index}
              style={{
                width: '100%',
                marginTop: '0.5rem',
                padding: '0.5rem',
                display: 'flex',
                flexDirection: 'column',
                border: '1px solid #e9ecf2',
                boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.1)',
                borderRadius: '0.8rem',
                background: 'linear-gradient(135deg, #FFFFFF 40%, #F5F5F5 60%)',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'start',
                }}
              >
                <div>
                  <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Switch
                      checked={!!getRow(sourceTableName)}
                      onCheckedChange={(state: boolean) =>
                        handleSwitch(state, sourceTableName)
                      }
                    />
                    <div
                      style={{
                        fontSize: 14,
                        overflow: 'hidden',
                        fontWeight: 'bold',
                        color: 'rgba(0,0,0,0.7)',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {sourceTableName}
                    </div>
                  </div>
                  {rows.find(
                    (row) => row.source === `${schema}.${sourceTableName}`
                  )?.destination && (
                    <div style={{ padding: '0.5rem' }}>
                      <RowWithTextField
                        label={
                          <div
                            style={{
                              marginTop: '0.5rem',
                              fontSize: 14,
                            }}
                          >
                            Destination Table Name
                            {RequiredIndicator(true)}
                          </div>
                        }
                        action={
                          <div
                            style={{
                              marginTop: '0.5rem',
                              display: 'flex',
                              flexDirection: 'row',
                              alignItems: 'center',
                            }}
                          >
                            <TextField
                              variant='simple'
                              defaultValue={
                                rows.find(
                                  (row) =>
                                    row.source ===
                                    `${schema}.${sourceTableName}`
                                )?.destination
                              }
                              onChange={(
                                e: React.ChangeEvent<HTMLInputElement>
                              ) =>
                                updateDestination(
                                  `${schema}.${sourceTableName}`,
                                  e.target.value
                                )
                              }
                            />
                          </div>
                        }
                      />
                      <RowWithTextField
                        label={
                          <div
                            style={{
                              marginTop: '0.5rem',
                              fontSize: 14,
                            }}
                          >
                            Partition Key
                          </div>
                        }
                        action={
                          <div
                            style={{
                              marginTop: '0.5rem',
                              display: 'flex',
                              flexDirection: 'row',
                              alignItems: 'center',
                            }}
                          >
                            <TextField
                              variant='simple'
                              onChange={(
                                e: React.ChangeEvent<HTMLInputElement>
                              ) =>
                                updatePartitionKey(
                                  `${schema}.${sourceTableName}`,
                                  e.target.value
                                )
                              }
                            />
                          </div>
                        }
                      />
                      <div style={{ fontSize: 14 }}>
                        This is used only if you enable initial load, and
                        specifies its watermark.
                      </div>
                    </div>
                  )}
                </div>
                <ColumnsDisplay
                  peerName={sourcePeerName}
                  schemaName={schema}
                  tableName={sourceTableName}
                  setColumns={setTableColumns}
                  columns={tableColumns}
                />
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
