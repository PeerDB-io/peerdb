'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction } from 'react';
import { TableMapRow } from '../types';

interface TableMappingProps {
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}
const TableMapping = ({ rows, setRows }: TableMappingProps) => {
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

  return (
    <div style={{ marginTop: '1rem' }}>
      <Label colorName='lowContrast'>Table Mapping</Label>
      <table>
        <thead>
          <tr>
            <th style={{ fontWeight: 'normal', fontSize: 14 }}>Source Table</th>
            <th style={{ fontWeight: 'normal', fontSize: 14 }}>
              Destination Table
            </th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              <td>
                <TextField
                  variant='simple'
                  value={row.source}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleTableChange(index, 'source', e.target.value)
                  }
                />
              </td>
              <td>
                <TextField
                  variant='simple'
                  value={row.destination}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleTableChange(index, 'destination', e.target.value)
                  }
                />
              </td>
              <td>
                <Button
                  variant='destructive'
                  onClick={() => handleRemoveRow(index)}
                  disabled={rows.length === 1}
                >
                  <Icon name='delete' />
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <Button
        variant='normalSolid'
        style={{ fontSize: 13, marginTop: '0.5rem' }}
        onClick={handleAddRow}
      >
        <Icon name='add' />
      </Button>
    </div>
  );
};

export default TableMapping;
