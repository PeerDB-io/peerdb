'use client';
import { DBType, dBTypeToJSON } from '@/grpc_generated/peers';
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
import { fetchSchemas, fetchTables } from './handlers';
import { expandableStyle, schemaBoxStyle, tableBoxStyle } from './styles';

interface TableMappingProps {
  sourcePeerName: string;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  schema: string;
  setSchema: Dispatch<SetStateAction<string>>;
  peerType?: DBType;
}

const TableMapping = ({
  sourcePeerName,
  rows,
  setRows,
  schema,
  setSchema,
  peerType,
}: TableMappingProps) => {
  const [allSchemas, setAllSchemas] = useState<string[]>();
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: string[] }[]
  >([]);
  const [loading, setLoading] = useState(false);

  const handleAddRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index] = { ...newRows[index], selected: true };
    setRows(newRows);
  };

  const handleRemoveRow = (source: string) => {
    const newRows = [...rows];
    const index = newRows.findIndex((row) => row.source === source);
    if (index >= 0) newRows[index] = { ...newRows[index], selected: false };
    setRows(newRows);
  };

  const handleTableSelect = (on: boolean, source: string) => {
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
    newRows[index] = { ...newRows[index], destination: dest };
    setRows(newRows);
  };

  const getTablesOfSchema = useCallback(
    (schemaName: string) => {
      fetchTables(sourcePeerName, schemaName, setLoading).then((tableNames) => {
        if (tableNames) {
          const newRows = [];
          for (const tableName of tableNames) {
            const dstName =
              peerType != undefined && dBTypeToJSON(peerType) == 'BIGQUERY'
                ? tableName
                : `${schemaName}.${tableName}`;
            newRows.push({
              source: `${schemaName}.${tableName}`,
              destination: dstName,
              partitionKey: '',
              exclude: [],
              selected: false,
            });
          }
          setRows(newRows);
        }
      });
    },
    [sourcePeerName, setRows, peerType]
  );

  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (peerType != undefined && dBTypeToJSON(peerType) == 'BIGQUERY') {
      setRows((rows) => {
        const newRows = [...rows];
        newRows.forEach((_, i) => {
          const row = newRows[i];
          newRows[i] = {
            ...row,
            destination: row.destination?.split('.')[1],
          };
        });
        return newRows;
      });
    } else {
      setRows((rows) => {
        const newRows = [...rows];
        newRows.forEach((_, i) => {
          const row = newRows[i];
          newRows[i] = {
            ...row,
            destination: `${schema}.${
              row.destination?.split('.')[1] || row.destination
            }`,
          };
        });
        return newRows;
      });
    }
  }, [peerType, setRows, schema]);

  useEffect(() => {
    fetchSchemas(sourcePeerName, setLoading).then((res) => setAllSchemas(res));
    setSchema('public');
    getTablesOfSchema('public');
  }, [sourcePeerName, setSchema, getTablesOfSchema]);

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label colorName='lowContrast'>Select tables to sync</Label>
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
                  <div style={{ ...expandableStyle, whiteSpace: 'nowrap' }}>
                    <Icon name='arrow_right' />
                    <p>{schema}</p>
                  </div>
                  <div className='ml-5 mt-3' style={{ width: '90%' }}>
                    {['oss1', 'oss2'].map((table, index) => {
                      return (
                        <div style={tableBoxStyle}>
                          <div
                            key={index}
                            style={{
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'space-between',
                            }}
                          >
                            <RowWithCheckbox
                              label={
                                <Label as='label' style={{ fontSize: 13 }}>
                                  {table}
                                </Label>
                              }
                              action={
                                <Checkbox
                                  onCheckedChange={(state: boolean) =>
                                    handleTableSelect(state, table)
                                  }
                                />
                              }
                            />

                            <div style={{ width: '40%' }}>
                              <TextField
                                style={{ fontSize: 12 }}
                                variant='simple'
                                placeholder={'Enter target table'}
                                //defaultValue={row.destination}
                                onChange={(
                                  e: React.ChangeEvent<HTMLInputElement>
                                ) => updateDestination(table, e.target.value)}
                              />
                            </div>
                          </div>
                          <div className='ml-5' style={{ width: '100%' }}>
                            <Label
                              as='label'
                              colorName='lowContrast'
                              style={{ fontSize: 13 }}
                            >
                              Columns
                            </Label>
                            {['id', 'name'].map((column, index) => (
                              <RowWithCheckbox
                                key={index}
                                label={
                                  <Label as='label' style={{ fontSize: 13 }}>
                                    {column}
                                  </Label>
                                }
                                action={<Checkbox />}
                              />
                            ))}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
                {/* <ColumnsDisplay
                    peerName={sourcePeerName}
                    schemaName={schema}
                    tableName={row.source.split('.')[1]}
                    setColumns={setTableColumns}
                    columns={tableColumns}
                  /> */}
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
