'use client';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { BarLoader } from 'react-spinners/';
import { TableMapRow } from '../../../dto/MirrorsDTO';
import { fetchSchemas } from '../handlers';
import SchemaBox from './schemabox';
import { loaderContainer } from './styles';

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
  const [schemaQuery, setSchemaQuery] = useState('');
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: string[] }[]
  >([]);
  useEffect(() => {
    fetchSchemas(sourcePeerName).then((res) => setAllSchemas(res));
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
            placeholder='Search for schemas'
            value={schemaQuery}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSchemaQuery(e.target.value)
            }
          />
        </div>
      </div>
      <div style={{ maxHeight: '70vh', overflow: 'scroll' }}>
        {allSchemas ? (
          allSchemas
            ?.filter((schema) => {
              return schema.toLowerCase().includes(schemaQuery.toLowerCase());
            })
            .map((schema) => (
              <SchemaBox
                key={schema}
                schema={schema}
                sourcePeer={sourcePeerName}
                rows={rows}
                setRows={setRows}
                tableColumns={tableColumns}
                setTableColumns={setTableColumns}
                peerType={peerType}
              />
            ))
        ) : (
          <div style={loaderContainer}>
            <BarLoader color='#36d7b7' width='40%' />
          </div>
        )}
      </div>
    </div>
  );
};

export default TableMapping;
