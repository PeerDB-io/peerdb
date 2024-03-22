'use client';
import { MirrorRowsData } from '@/app/dto/MirrorsDTO';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useMemo, useState } from 'react';

const TableStats = ({ tableSyncs }: { tableSyncs: MirrorRowsData[] }) => {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const tableDataToShow = useMemo(() => {
    return tableSyncs.filter((tableSync) =>
      tableSync.destinationTableName
        .toLowerCase()
        .includes(searchQuery.toLowerCase())
    );
  }, [tableSyncs, searchQuery]);

  return (
    <div style={{ marginTop: '2rem', marginBottom: '2rem' }}>
      <Table
        title={<Label variant='headline'>Table Stats</Label>}
        toolbar={{
          right: (
            <SearchField
              placeholder='Search for table'
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setSearchQuery(e.target.value)
              }
            />
          ),
        }}
        header={
          <TableRow>
            {['Table Name', 'Total', 'Inserts', 'Updates', 'Deletes'].map(
              (heading, index) => (
                <TableCell as='th' key={index}>
                  <Label as='label' style={{ fontWeight: 'bold' }}>
                    {heading}
                  </Label>
                </TableCell>
              )
            )}
          </TableRow>
        }
      >
        {tableDataToShow.map((tableSync) => {
          return (
            <TableRow key={tableSync.destinationTableName}>
              <TableCell>
                <Label>{tableSync.destinationTableName}</Label>
              </TableCell>
              <TableCell>
                <Label>{tableSync.totalCount}</Label>
              </TableCell>
              <TableCell>
                <Label>{tableSync.insertCount}</Label>
              </TableCell>
              <TableCell>
                <Label>{tableSync.updateCount}</Label>
              </TableCell>
              <TableCell>
                <Label>{tableSync.deleteCount}</Label>
              </TableCell>
            </TableRow>
          );
        })}
      </Table>
    </div>
  );
};

export default TableStats;
