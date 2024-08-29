'use client';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Callout } from '@tremor/react';
import Link from 'next/link';
import { Dispatch, SetStateAction, useEffect, useMemo, useState } from 'react';
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
  // schema -> omitted source table mapping
  omitAdditionalTablesMapping: Map<string, string[]>;
  initialLoadOnly: boolean;
}

const TableMapping = ({
  sourcePeerName,
  rows,
  setRows,
  peerType,
  omitAdditionalTablesMapping,
  initialLoadOnly,
}: TableMappingProps) => {
  const [allSchemas, setAllSchemas] = useState<string[]>();
  const [schemaQuery, setSchemaQuery] = useState('');
  const [tableColumns, setTableColumns] = useState<
    { tableName: string; columns: string[] }[]
  >([]);
  const searchedSchemas = useMemo(() => {
    return allSchemas?.filter((schema) => {
      return schema.toLowerCase().includes(schemaQuery.toLowerCase());
    });
  }, [allSchemas, schemaQuery]);

  useEffect(() => {
    fetchSchemas(sourcePeerName).then((res) => setAllSchemas(res));
  }, [sourcePeerName, initialLoadOnly]);

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label as='label' colorName='lowContrast' style={{ fontSize: 16 }}>
        Select tables to sync
      </Label>
      <br></br>
      <Label as='label' style={{ fontSize: 15 }}>
        Before selecting tables, please make sure that{' '}
        <Link
          style={{ color: 'teal' }}
          target='_blank'
          href={
            'https://docs.peerdb.io/connect/postgres/rds_postgres#creating-peerdb-user-and-granting-permissions'
          }
        >
          these permissions
        </Link>{' '}
        have been granted for your tables.
      </Label>
      {peerType === DBType.EVENTHUBS && (
        <Callout
          title='Note on Eventhubs targets'
          color='grey'
          style={{ fontSize: 14 }}
        >
          Eventhubs targets are of the form{' '}
          <b>namespace.eventhub_name.partition_column.</b>
          <br></br>
          Namespaces are specified in the Eventhub peer. PeerDB will create the
          eventhub if needed with the name you specify in the provided namespace
          for each source table <br></br>
          Messages are sent to partitions based on the values of the partition
          column.
        </Callout>
      )}
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
        {searchedSchemas ? (
          searchedSchemas.map((schema) => (
            <SchemaBox
              key={schema}
              schema={schema}
              sourcePeer={sourcePeerName}
              rows={rows}
              setRows={setRows}
              tableColumns={tableColumns}
              setTableColumns={setTableColumns}
              peerType={peerType}
              omitAdditionalTables={omitAdditionalTablesMapping.get(schema)}
              initialLoadOnly={initialLoadOnly}
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
