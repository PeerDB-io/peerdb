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
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: string[] }[]
  >([]);
  const [loading, setLoading] = useState(false);

  const handleAddRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index].selected = true;
  };

  const handleRemoveRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index].selected = false;
    setRows(newRows);
  };

  const handleSelectAll = (
    e: React.MouseEvent<HTMLInputElement, MouseEvent>
  ) => {
    const newRows = [...rows];
    newRows.forEach((_, i) => {
      newRows[i].selected = e.currentTarget.checked;
    });
    setRows(newRows);
  };

  const handleSwitch = (on: boolean, source: string) => {
    if (on) {
      handleAddRow(source);
    } else {
      handleRemoveRow(source);
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
      fetchTables(sourcePeerName, schemaName, setLoading).then((tableNames) =>
        setRows((curr) => {
          const newRows = [...curr];
          tableNames.forEach((tableName) => {
            const row = newRows.find(
              (row) => row.source === `${schemaName}.${tableName}`
            );
            if (!row) {
              newRows.push({
                source: `${schemaName}.${tableName}`,
                destination: `${schemaName}.${tableName}`,
                partitionKey: '',
                selected: false,
              });
            }
          });
          return newRows;
        })
      );
    },
    [sourcePeerName, setRows]
  );

  const [searchQuery, setSearchQuery] = useState('');

  const filteredRows = rows?.filter((row) => {
    return row.source.toLowerCase().includes(searchQuery.toLowerCase());
  });

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
        {filteredRows ? (
          filteredRows.map((row, index) => (
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
                      checked={row.selected}
                      onCheckedChange={(state: boolean) =>
                        handleSwitch(state, row.source)
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
                      {row.source}
                    </div>
                  </div>
                  {row.selected && (
                    <div style={{ padding: '0.5rem' }}>
                      <RowWithTextField
                        key={row.source}
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
                              defaultValue={row.destination}
                              onChange={(
                                e: React.ChangeEvent<HTMLInputElement>
                              ) =>
                                updateDestination(row.source, e.target.value)
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
                                updatePartitionKey(row.source, e.target.value)
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
                  tableName={row.source.split('.')[1]}
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
