'use client';
import { Label } from '@/lib/Label';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { TableMapRow } from '../../dto/MirrorsDTO';
import { RowWithSelect } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { fetchSchemas, fetchTables } from './handlers';
import { Switch } from '@/lib/Switch';
import ColumnsDisplay from './columns';

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
  const [tableColumns, setTableColumns] = useState<{tableName:string,columns:string[]}[]>([])
  const [loading, setLoading] = useState(false);

  const getTablesOfSchema = (schemaName:string) => {
    fetchTables(sourcePeerName,schemaName,setLoading).then(res => setAllTables(res))
  }

  useEffect(()=>{
    fetchSchemas(sourcePeerName,setLoading).then(res => setAllSchemas(res))
    getTablesOfSchema("public")
  },[])

  return (
    <div style={{ marginTop: '1rem' }}> 
    <Label colorName='lowContrast'>Select tables to sync</Label>
      <RowWithSelect
        label={
          <Label>
            Source Schema
          </Label>
        }
        action={
        <Select
          placeholder="Select a schema"
          onValueChange={(val:string) => {
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
          marginTop:"0.5rem",
          padding:"0.5rem",
          display:"flex",
          flexDirection:"column",
          border:"1px solid #e9ecf2",
          boxShadow:"0px 2px 4px rgba(0, 0, 0, 0.1)",
          borderRadius:"0.8rem",
          background: 'linear-gradient(135deg, #FFFFFF 40%, #F5F5F5 60%)'
        }}>
          <div style={{display:"flex",justifyContent:"space-between", alignItems:'start'}}>
            <div style={{display:"flex",alignItems:'center'}}>
            <Switch/>
            <div style={{
            fontSize:14,
            overflow:"hidden",
            fontWeight:"bold",
            color:"rgba(0,0,0,0.7)",
            textOverflow:"ellipsis",
            whiteSpace:"nowrap"}}>{sourceTableName}</div>
            </div>
           <ColumnsDisplay 
            peerName={sourcePeerName}
            schemaName={schema}
            tableName={sourceTableName}
            setColumns={setTableColumns} 
            columns={tableColumns}/>
          </div>
        </div>
      )):<p>Loading tables</p>}
      </div>
    </div>
  );
};

export default TableMapping;
