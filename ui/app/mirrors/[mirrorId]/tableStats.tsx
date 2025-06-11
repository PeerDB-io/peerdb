'use client';
import { CDCTableRowCounts } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useMemo, useState } from 'react';
import { RowDataFormatter } from './rowsDisplay';

export default function TableStats({
  tableSyncs,
}: {
  tableSyncs: CDCTableRowCounts[];
}) {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const tableDataToShow = useMemo(() => {
    return tableSyncs.filter((tableSync) =>
      tableSync.tableName.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [tableSyncs, searchQuery]);

  return (
    <div style={{ marginTop: '2rem', marginBottom: '2rem' }}>
      <Label variant='headline'>Table Stats</Label>
      <div style={{ maxHeight: '30vh', overflow: 'auto' }}>
        <Table
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
              <TableRow key={tableSync.tableName}>
                <TableCell>
                  <Label>{tableSync.tableName}</Label>
                </TableCell>
                {[
                  tableSync.counts?.totalCount,
                  tableSync.counts?.insertsCount,
                  tableSync.counts?.updatesCount,
                  tableSync.counts?.deletesCount,
                ].map((rowMetric, id) => {
                  return (
                    <TableCell key={id}>
                      <Label>{RowDataFormatter(rowMetric ?? 0)}</Label>
                    </TableCell>
                  );
                })}
              </TableRow>
            );
          })}
        </Table>
      </div>
    </div>
  );
}
