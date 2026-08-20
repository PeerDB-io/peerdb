'use client';

import { humanizeThresholds } from '@/app/utils/momentOptions';
import TimeLabel from '@/components/TimeComponent';
import {
  CDCBatch,
  CDCBatchStatus,
  GetCDCBatchesRequest,
  GetCDCBatchesResponse,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { useSortButtonColor } from '@/lib/hooks/useSortButtonColor';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { Tooltip } from '@/lib/Tooltip';
import moment from 'moment';
import { useCallback, useEffect, useState } from 'react';
import { RowDataFormatter } from './rowsDisplay';

type SyncStatusTableProps = { mirrorName: string };

function TimeWithDurationOrRunning({
  startTime,
  endTime,
}: {
  startTime: Date;
  endTime: Date | undefined;
}) {
  if (endTime) {
    return (
      <>
        <TimeLabel timeVal={moment(endTime).format('YYYY-MM-DD HH:mm:ss')} />
        <Label>
          (
          {moment
            .duration(moment(endTime).diff(startTime))
            .humanize(humanizeThresholds)}
          )
        </Label>
      </>
    );
  } else {
    return (
      <Label>
        <ProgressCircle variant='determinate_progress_circle' />
      </Label>
    );
  }
}

function BatchStatus({ batch }: { batch: CDCBatch }) {
  switch (batch.status) {
    case CDCBatchStatus.CDC_BATCH_STATUS_PARTIAL: {
      const progress =
        batch.tablesTotal > 0
          ? ` · ${batch.tablesCompleted}/${batch.tablesTotal} tables`
          : '';
      const laggingTables =
        batch.laggingTables.length > 0
          ? `Lagging tables: ${batch.laggingTables.join(', ')}`
          : 'Some tables have not reached this batch yet';
      return (
        <Tooltip content={laggingTables}>
          <Label
            style={{
              backgroundColor: '#fef3c7',
              borderRadius: '999px',
              color: '#92400e',
              padding: '2px 8px',
            }}
          >
            Partial{progress}
          </Label>
        </Tooltip>
      );
    }
    case CDCBatchStatus.CDC_BATCH_STATUS_COMPLETED:
      return <Label>Completed</Label>;
    case CDCBatchStatus.CDC_BATCH_STATUS_RUNNING:
    case CDCBatchStatus.CDC_BATCH_STATUS_UNSPECIFIED:
    case CDCBatchStatus.UNRECOGNIZED:
      return <Label>Running</Label>;
  }
}

const ROWS_PER_PAGE = 5;
export function SyncStatusTable({ mirrorName }: SyncStatusTableProps) {
  const sortButtonColor = useSortButtonColor();
  const [totalPages, setTotalPages] = useState(1);
  const [currentPage, setCurrentPage] = useState(1);
  const [ascending, setAscending] = useState(false);
  const [[beforeId, afterId], setBeforeAfterId] = useState([-1, -1]);
  const [batches, setBatches] = useState<CDCBatch[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      const req: GetCDCBatchesRequest = {
        flowJobName: mirrorName,
        limit: ROWS_PER_PAGE,
        beforeId: beforeId,
        afterId: afterId,
        ascending,
      };
      const res = await fetch('/api/v1/mirrors/cdc/batches', {
        method: 'POST',
        cache: 'no-store',
        body: JSON.stringify(req),
      });
      const data: GetCDCBatchesResponse = await res.json();
      setBatches(data.cdcBatches ?? []);
      setCurrentPage(data.page);
      setTotalPages(Math.ceil(data.total / req.limit));
    };

    fetchData();

    const refresh = window.setInterval(fetchData, 10_000);
    return () => window.clearInterval(refresh);
  }, [mirrorName, beforeId, afterId, ascending]);

  const nextPage = useCallback(() => {
    if (batches.length === 0) {
      setBeforeAfterId([-1, ascending ? 0 : -1]);
    } else if (ascending) {
      setBeforeAfterId([-1, batches[batches.length - 1].batchId]);
    } else {
      setBeforeAfterId([batches[batches.length - 1].batchId, -1]);
    }
  }, [batches, ascending]);
  const prevPage = useCallback(() => {
    if (batches.length === 0 || currentPage < 3) {
      setBeforeAfterId([-1, ascending ? 0 : -1]);
    } else if (ascending) {
      setBeforeAfterId([batches[0].batchId, -1]);
    } else {
      setBeforeAfterId([-1, batches[0].batchId]);
    }
  }, [batches, ascending, currentPage]);

  return (
    <Table
      title={<Label variant='headline'>CDC Syncs</Label>}
      toolbar={{
        left: (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <Button variant='normalBorderless' onClick={prevPage}>
              <Icon name='chevron_left' />
            </Button>
            <Button variant='normalBorderless' onClick={nextPage}>
              <Icon name='chevron_right' />
            </Button>
            <Label>{`${currentPage} of ${totalPages}`}</Label>
            <Button
              variant='normalBorderless'
              onClick={() => window.location.reload()}
            >
              <Icon name='refresh' />
            </Button>
            <button
              className='IconButton'
              onClick={() => {
                setAscending(true);
                setBeforeAfterId([-1, 0]);
              }}
              aria-label='sort up'
              style={{ color: sortButtonColor(ascending) }}
            >
              <Icon name='arrow_upward' />
            </button>
            <button
              className='IconButton'
              onClick={() => {
                setAscending(false);
                setBeforeAfterId([-1, -1]);
              }}
              aria-label='sort down'
              style={{ color: sortButtonColor(!ascending) }}
            >
              <Icon name='arrow_downward' />
            </button>
          </div>
        ),
      }}
      header={
        <TableRow>
          {[
            'Batch ID',
            'Status',
            'Start Time',
            'End Time (Duration)',
            'Rows Synced',
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
      {batches.map((row) => (
        <TableRow key={row.batchId}>
          <TableCell>
            <Label>{Number(row.batchId)}</Label>
          </TableCell>
          <TableCell>
            <BatchStatus batch={row} />
          </TableCell>
          <TableCell>
            <Label>
              {
                <TimeLabel
                  timeVal={moment(row.startTime).format('YYYY-MM-DD HH:mm:ss')}
                />
              }
            </Label>
          </TableCell>
          <TableCell>
            <TimeWithDurationOrRunning
              startTime={row.startTime!}
              endTime={row.endTime}
            />
          </TableCell>
          <TableCell>{RowDataFormatter(row.numRows)}</TableCell>
        </TableRow>
      ))}
    </Table>
  );
}
