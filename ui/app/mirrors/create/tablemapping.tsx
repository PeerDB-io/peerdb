'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { TextField } from '@/lib/TextField';
import { TextInput } from '@tremor/react';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { TableMapRow } from '../../dto/MirrorsDTO';
import { RowWithSelect } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { fetchColumns, fetchSchemas, fetchTables } from './handlers';
import { Switch } from '@/lib/Switch';

interface TableMappingProps {
  sourcePeerName: string;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
  schema: string;
  setSchema:Dispatch<SetStateAction<string>>;
}
const TableMapping = ({ sourcePeerName, rows, setRows, schema, setSchema }: TableMappingProps) => {
  const [allSchemas, setAllSchemas] = useState<string[]>()
  const [allTables, setAllTables] = useState<string[]>()
  const [allColumns, setAllColumns] = useState<{tableName:string,columns:string[]}>()
  const [loading, setLoading] = useState(false);

  const handleAddRow = () => {
    setRows([...rows, { source: '', destination: '' }]);
  };

  const handleRemoveRow = (index: number) => {
    if (rows.length === 1) {
      return;
    }
    const newRows = [...rows];
    newRows.splice(index, 1);
    setRows(newRows);
  };

  const handleTableChange = (
    index: number,
    field: 'source' | 'destination',
    value: string
  ) => {
    const newRows = [...rows];
    newRows[index][field] = value;
    setRows(newRows);
  };

  const getTablesOfSchema = (schemaName:string) => {
    fetchTables(sourcePeerName,schemaName,setLoading).then(res => setAllTables(res))
  }

  const getColumnsOfTable = (tableName:string) => {
    fetchColumns(sourcePeerName,schema,tableName,setLoading).then(res => setAllColumns({tableName:tableName,columns:res}))
  }

  useEffect(()=>{
    fetchSchemas(sourcePeerName,setLoading).then(res => setAllSchemas(res))
    getTablesOfSchema("public")
  },[])

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label colorName='lowContrast'>Table Mapping</Label>
      <RowWithSelect
        label={
          <Label>
            Source Schema
          </Label>
        }
        action={
        <Select
          placeholder="Select a schema"
          onValueChange={(val) => {
            setSchema(val)
            getTablesOfSchema(val)
          }}
          defaultValue={schema.length>0?schema:"Loading..."}
        >
          {/*<TextInput placeholder='Search schema..'/>*/}
          {allSchemas? allSchemas.map((schemaName, id) => {
            return (
              <SelectItem key={id} value={schemaName}>
                {schemaName}
              </SelectItem>
            );
          }):<p>Loading schemas...</p>}
        </Select>
        
        }
      />
      <div style={{maxHeight:"30vh", overflow:"scroll"}}>
      {allTables? allTables.map((sourceTableName, index) => (
        <div key={index} style={{
          width:"100%",
          marginTop:"1rem",
          padding:"1rem",
          display:"flex",
          flexDirection:"column",
          border:"1px solid #e9ecf2",
          boxShadow:"0px 2px 4px rgba(0, 0, 0, 0.1)",
          borderRadius:"0.8rem",
            background: 'linear-gradient(135deg, #FFFFFF 40%, #F5F5F5 60%)'
        }}>
          <div style={{display:"flex",justifyContent:"space-between", alignItems:'center'}}>
            <div style={{display:"flex",alignItems:'center'}}>
            <Switch/>
            <div style={{fontSize:15}}>{sourceTableName}</div>
            </div>
            <div style={{display:"flex",flexDirection:'column'}}>
              <Button 
                onClick={() => getColumnsOfTable(sourceTableName)}
                style={{
                  height:"20%",
                  backgroundColor:"white",
                  boxShadow:"0px 2px 4px rgba(0, 0, 0, 0.2)",
                  fontSize:14}}
              >Show Columns
              </Button>

              <div>
                {allColumns && allColumns.tableName === sourceTableName && allColumns.columns.map((column, id) => {
                  return (
                    <div style={{fontSize:14}}>{column}</div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      )):<p>Loading tables</p>}
      </div>
    </div>
  );
};

export default TableMapping;
