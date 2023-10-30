'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';
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
    newRows.splice(index, 1);
    setRows(newRows);
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

  const getTablesOfSchema = (schemaName: string) => {
    fetchTables(sourcePeerName, schemaName, setLoading).then((res) =>
      setAllTables(res)
    );
  };

  useEffect(() => {
    fetchSchemas(sourcePeerName, setLoading).then((res) => setAllSchemas(res));
    getTablesOfSchema('public');
  }, []);

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
            defaultValue={schema.length > 0 ? schema : 'Loading...'}
          >
            {/*<TextInput placeholder='Search schema..'/>*/}
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

      <div style={{ maxHeight: '30vh', overflow: 'scroll' }}>
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
                        This is required if you enable initial load, and
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
          <p>Loading tables</p>
        )}
      </div>
    </div>
  );
};

export default TableMapping;
