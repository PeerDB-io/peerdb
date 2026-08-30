'use client';
import { fetcher } from '@/app/utils/swr';
import TimeLabel from '@/components/TimeComponent';
import { GetTableReplicationStateResponse } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useMemo, useState } from 'react';
import useSWR from 'swr';
import { RowDataFormatter } from './rowsDisplay';

type TableReplicationStateProps = {
  flowJobName: string;
};

function TimestampOrNever({ time }: { time: Date | undefined }) {
  if (!time) {
    return <Label>Never</Label>;
  }
  return <TimeLabel timeVal={moment(time).format('YYYY-MM-DD HH:mm:ss')} />;
}

export default function TableReplicationState({
  flowJobName,
}: TableReplicationStateProps) {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const {
    data: response,
    error,
    isLoading,
  } = useSWR<GetTableReplicationStateResponse>(
    `/api/v1/mirrors/cdc/table_replication_state/${encodeURIComponent(flowJobName)}`,
    fetcher
  );

  const tables = useMemo(
    () =>
      (response?.tables ?? []).filter((table) =>
        table.sourceTableIdentifier
          .toLowerCase()
          .includes(searchQuery.toLowerCase())
      ),
    [response, searchQuery]
  );

  if (isLoading) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }

  if (error || !response) {
    return <Label>Unable to load table replication state.</Label>;
  }

  return (
    <div style={{ marginTop: '2rem', marginBottom: '2rem' }}>
      <Label variant='headline'>Table Replication State</Label>
      <div style={{ maxHeight: '60vh', overflow: 'auto' }}>
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
              {[
                'Table Name',
                'Cursor',
                'Last Attempt At',
                'Last Synced At',
                'Synced Batch ID',
                'Normalized Batch ID',
                'Last Normalized At',
                'Inserts',
                'Updates',
                'Deletes',
              ].map((heading, index) => (
                <TableCell as='th' key={index}>
                  <Label as='label' style={{ fontWeight: 'bold' }}>
                    {heading}
                  </Label>
                </TableCell>
              ))}
            </TableRow>
          }
        >
          {tables.map((table) => (
            <TableRow key={table.sourceTableIdentifier}>
              <TableCell>
                <Label>{table.sourceTableIdentifier}</Label>
              </TableCell>
              <TableCell>
                <Label>{table.cursorText || '-'}</Label>
              </TableCell>
              <TableCell>
                <TimestampOrNever time={table.lastAttemptAt} />
              </TableCell>
              <TableCell>
                <TimestampOrNever time={table.lastSyncedAt} />
              </TableCell>
              <TableCell>
                <Label>{Number(table.syncedBatchId)}</Label>
              </TableCell>
              <TableCell>
                <Label>{Number(table.normalizedBatchId)}</Label>
              </TableCell>
              <TableCell>
                <TimestampOrNever time={table.lastNormalizedAt} />
              </TableCell>
              <TableCell>
                <Label>{RowDataFormatter(table.insertsCount)}</Label>
              </TableCell>
              <TableCell>
                <Label>{RowDataFormatter(table.updatesCount)}</Label>
              </TableCell>
              <TableCell>
                <Label>{RowDataFormatter(table.deletesCount)}</Label>
              </TableCell>
            </TableRow>
          ))}
        </Table>
      </div>
    </div>
  );
}
