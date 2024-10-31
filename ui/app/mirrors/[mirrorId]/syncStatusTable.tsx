'use client';

import SelectTheme from '@/app/styles/select';
import TimeLabel from '@/components/TimeComponent';
import {
  CDCBatch,
  GetCDCBatchesRequest,
  GetCDCBatchesResponse,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useCallback, useEffect, useState } from 'react';
import ReactSelect from 'react-select';
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
          {moment.duration(moment(endTime).diff(startTime)).humanize({ ss: 1 })}
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

const ROWS_PER_PAGE = 5;
const sortOptions = [
  { value: 'batchId', label: 'Batch ID' },
  { value: 'startTime', label: 'Start Time' },
  { value: 'endTime', label: 'End Time' },
  { value: 'numRows', label: 'Rows Synced' },
];

export const SyncStatusTable = ({ mirrorName }: SyncStatusTableProps) => {
  const [sortField, setSortField] = useState<
    'startTime' | 'endTime' | 'numRows' | 'batchId'
  >('batchId');

  const [totalPages, setTotalPages] = useState(1);
  const [currentPage, setCurrentPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState(-1);
  const [descending, setDescending] = useState(false);
  const [[beforeId, afterId], setBeforeAfterId] = useState([-1, -1]);
  const [batches, setBatches] = useState<CDCBatch[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      const req: GetCDCBatchesRequest = {
        flowJobName: mirrorName,
        limit: ROWS_PER_PAGE,
        // TODO descending, sortField, searchQuery
        beforeId: beforeId,
        afterId: afterId,
      };
      const res = await fetch('/api/v1/mirrors/cdc/batches', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        cache: 'no-store',
        body: JSON.stringify(req),
      });
      const data: GetCDCBatchesResponse = await res.json();
      setBatches(data.cdcBatches);
      setCurrentPage(data.page);
      setTotalPages(Math.ceil(data.total / req.limit));
    };

    fetchData();
  }, [mirrorName, descending, sortField, searchQuery, beforeId, afterId]);

  const nextPage = useCallback(() => {
    if (batches.length === 0) {
      setBeforeAfterId([-1, -1]);
    }
    setBeforeAfterId([batches[batches.length - 1].batchId, -1]);
  }, [batches]);
  const prevPage = useCallback(() => {
    if (batches.length === 0 || currentPage < 3) {
      setBeforeAfterId([-1, -1]);
    }
    setBeforeAfterId([-1, batches[0].batchId]);
  }, [batches, currentPage]);

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
            <div style={{ minWidth: '10em' }}>
              <ReactSelect
                options={sortOptions}
                value={{
                  value: sortField,
                  label: sortOptions.find((opt) => opt.value === sortField)
                    ?.label,
                }}
                onChange={(val, _) => {
                  const sortVal =
                    (val?.value as
                      | 'startTime'
                      | 'endTime'
                      | 'numRows'
                      | 'batchId') ?? 'batchId';
                  setSortField(sortVal);
                }}
                defaultValue={{ value: 'batchId', label: 'Batch ID' }}
                theme={SelectTheme}
              />
            </div>
            <button
              className='IconButton'
              onClick={() => setDescending(false)}
              aria-label='sort up'
              style={{ color: descending ? 'gray' : 'green' }}
            >
              <Icon name='arrow_upward' />
            </button>
            <button
              className='IconButton'
              onClick={() => setDescending(true)}
              aria-label='sort down'
              style={{ color: descending ? 'green' : 'gray' }}
            >
              <Icon name='arrow_downward' />
            </button>
          </div>
        ),
        right: (
          <SearchField
            placeholder='Search by batch ID'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(+e.target.value)
            }
          />
        ),
      }}
      header={
        <TableRow>
          {['Batch ID', 'Start Time', 'End Time (Duration)', 'Rows Synced'].map(
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
      {batches.map((row) => (
        <TableRow key={row.batchId}>
          <TableCell>
            <Label>{Number(row.batchId)}</Label>
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
};
