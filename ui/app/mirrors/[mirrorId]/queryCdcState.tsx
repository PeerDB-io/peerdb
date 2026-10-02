'use client';
import TimeLabel from '@/components/TimeComponent';
import {
  GetQueryCDCReplicationStateResponse,
  QueryCDCReplicationState,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useEffect, useMemo, useState } from 'react';
import { RowDataFormatter } from './rowsDisplay';

function timeCell(time?: Date | string) {
  return time ? (
    <TimeLabel timeVal={moment(time).format('YYYY-MM-DD HH:mm:ss')} />
  ) : (
    'N/A'
  );
}

export default function QueryCdcStateTable({
  flowJobName,
}: {
  flowJobName: string;
}) {
  const [tables, setTables] = useState<QueryCDCReplicationState[]>();
  const [error, setError] = useState<string>();
  const [searchQuery, setSearchQuery] = useState('');

  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    fetch(
      `/api/v1/mirrors/cdc/query_cdc_replication_state/${encodeURIComponent(flowJobName)}`,
      { cache: 'no-store' }
    )
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(res.statusText);
        }
        const body: GetQueryCDCReplicationStateResponse = await res.json();
        if (!cancelled) {
          setTables(body.tables ?? []);
          setError(undefined);
        }
      })
      .catch((err: any) => {
        if (!cancelled) {
          setError(err.message ?? 'Failed to load replication state');
        }
      });
    return () => {
      cancelled = true;
    };
  }, [flowJobName, refreshKey]);

  const shownTables = useMemo(() => {
    const query = searchQuery.toLowerCase();
    return (tables ?? [])
      .filter((t) => t.sourceTableIdentifier.toLowerCase().includes(query))
      .sort((a, b) =>
        a.sourceTableIdentifier.localeCompare(b.sourceTableIdentifier)
      );
  }, [tables, searchQuery]);

  return (
    <Table
      title={<Label variant='headline'>Query CDC state</Label>}
      toolbar={{
        left: (
          <Button
            variant='normalBorderless'
            onClick={() => setRefreshKey((k) => k + 1)}
          >
            <Icon name='refresh' />
          </Button>
        ),
        right: (
          <SearchField
            placeholder='Search by table name'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(e.target.value)
            }
          />
        ),
      }}
      header={
        <TableRow>
          {[
            'Source Table',
            'Cursor',
            'Last Attempt',
            'Last Synced',
            'Synced Batch',
            'Last Normalized',
            'Normalized Batch',
            'Inserts',
            'Updates',
            'Deletes',
          ].map((header) => (
            <TableCell key={header} as='th'>
              {header}
            </TableCell>
          ))}
        </TableRow>
      }
    >
      {error ? (
        <TableRow>
          <TableCell>
            <Label colorName='lowContrast'>{error}</Label>
          </TableCell>
        </TableRow>
      ) : (
        shownTables.map((t) => (
          <TableRow key={t.sourceTableIdentifier}>
            <TableCell>
              <Label>{t.sourceTableIdentifier}</Label>
            </TableCell>
            <TableCell>
              <Label>{t.cursorText || 'N/A'}</Label>
            </TableCell>
            <TableCell>{timeCell(t.lastAttemptAt)}</TableCell>
            <TableCell>{timeCell(t.lastSyncedAt)}</TableCell>
            <TableCell>
              <Label>{t.syncedBatchId}</Label>
            </TableCell>
            <TableCell>{timeCell(t.lastNormalizedAt)}</TableCell>
            <TableCell>
              <Label>{t.normalizedBatchId}</Label>
            </TableCell>
            <TableCell>{RowDataFormatter(t.insertsCount)}</TableCell>
            <TableCell>{RowDataFormatter(t.updatesCount)}</TableCell>
            <TableCell>{RowDataFormatter(t.deletesCount)}</TableCell>
          </TableRow>
        ))
      )}
    </Table>
  );
}
