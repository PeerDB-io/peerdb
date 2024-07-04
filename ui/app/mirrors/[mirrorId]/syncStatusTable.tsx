'use client';

import SelectTheme from '@/app/styles/select';
import TimeLabel from '@/components/TimeComponent';
import { CDCBatch } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useMemo, useState } from 'react';
import ReactSelect from 'react-select';
import { RowDataFormatter } from './rowsDisplay';

type SyncStatusTableProps = {
  rows: CDCBatch[];
};

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

export const SyncStatusTable = ({ rows }: SyncStatusTableProps) => {
  const [currentPage, setCurrentPage] = useState(1);
  const [sortField, setSortField] = useState<
    'startTime' | 'endTime' | 'numRows' | 'batchId'
  >('batchId');

  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('dsc');
  const totalPages = Math.ceil(rows.length / ROWS_PER_PAGE);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const displayedRows = useMemo(() => {
    const searchRows = rows.filter(
      (row: any) => row.batchId == parseInt(searchQuery, 10)
    );
    const shownRows = searchRows.length > 0 ? searchRows : rows;
    shownRows.sort((a, b) => {
      const aValue = a[sortField];
      const bValue = b[sortField];
      if (aValue === undefined || bValue === undefined) {
        return 0;
      }

      if (aValue < bValue) {
        return sortDir === 'dsc' ? 1 : -1;
      } else if (aValue > bValue) {
        return sortDir === 'dsc' ? -1 : 1;
      } else {
        return 0;
      }
    });

    const startRow = (currentPage - 1) * ROWS_PER_PAGE;
    const endRow = startRow + ROWS_PER_PAGE;
    return shownRows.length > ROWS_PER_PAGE
      ? shownRows.slice(startRow, endRow)
      : shownRows;
  }, [searchQuery, currentPage, rows, sortField, sortDir]);

  const handlePrevPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
    }
  };

  const handleNextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1);
    }
  };

  return (
    <Table
      title={<Label variant='headline'>CDC Syncs</Label>}
      toolbar={{
        left: (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <Button variant='normalBorderless' onClick={handlePrevPage}>
              <Icon name='chevron_left' />
            </Button>
            <Button variant='normalBorderless' onClick={handleNextPage}>
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
              onClick={() => setSortDir('asc')}
              aria-label='sort up'
              style={{ color: sortDir == 'asc' ? 'green' : 'gray' }}
            >
              <Icon name='arrow_upward' />
            </button>
            <button
              className='IconButton'
              onClick={() => setSortDir('dsc')}
              aria-label='sort down'
              style={{ color: sortDir == 'dsc' ? 'green' : 'gray' }}
            >
              <Icon name='arrow_downward' />
            </button>
          </div>
        ),
        right: (
          <SearchField
            placeholder='Search by batch ID'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(e.target.value)
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
      {displayedRows.map((row) => (
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
