'use client';

import TimeLabel from '@/components/TimeComponent';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useCallback, useEffect, useMemo, useState } from 'react';
import ReactSelect from 'react-select';
type SyncStatusRow = {
  batchId: number;
  startTime: Date;
  endTime: Date | null;
  numRows: number;
};

type SyncStatusTableProps = {
  rows: SyncStatusRow[];
};

function TimeWithDurationOrRunning({
  startTime,
  endTime,
}: {
  startTime: Date;
  endTime: Date | null;
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

const ROWS_PER_PAGE = 10;
const sortOptions = [
  { value: 'startTime', label: 'Start Time' },
  { value: 'endTime', label: 'End Time' },
  { value: 'numRows', label: 'Rows Synced' },
];
export const SyncStatusTable = ({ rows }: SyncStatusTableProps) => {
  const [currentPage, setCurrentPage] = useState(1);
  const [sortField, setSortField] = useState<
    'startTime' | 'endTime' | 'numRows'
  >('startTime');
  const totalPages = Math.ceil(rows.length / ROWS_PER_PAGE);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const startRow = (currentPage - 1) * ROWS_PER_PAGE;
  const endRow = startRow + ROWS_PER_PAGE;
  const displayedRows = useMemo(() => {
    const allRows = rows.slice(startRow, endRow);
    const shownRows = allRows.filter(
      (row: any) => row.batchId == parseInt(searchQuery, 10)
    );
    return shownRows.length > 0 ? shownRows : allRows;
  }, [searchQuery, endRow, startRow, rows]);
  const handlePrevPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
      const newStartRow = (currentPage - 2) * ROWS_PER_PAGE;
      const newEndRow = newStartRow + ROWS_PER_PAGE;
      setDisplayedRows(rows.slice(newStartRow, newEndRow));
    }
  };

  const handleNextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1);
      const newStartRow = currentPage * ROWS_PER_PAGE;
      const newEndRow = newStartRow + ROWS_PER_PAGE;
      setDisplayedRows(rows.slice(newStartRow, newEndRow));
    }
  };

  const handleSort = useCallback(
    (sortOption: 'startTime' | 'endTime' | 'numRows') => {
      setDisplayedRows((currRows) =>
        [...currRows].sort((a, b) => {
          const aValue = a[sortOption];
          const bValue = b[sortOption];
          if (aValue === null || bValue === null) {
            return 0;
          }

          if (aValue < bValue) {
            return -1;
          } else if (aValue > bValue) {
            return 1;
          } else {
            return 0;
          }
        })
      );
    },
    [setDisplayedRows]
  );

  useEffect(() => {
    handleSort(sortField);
  }, [handleSort, currentPage, sortField]);

  return (
    <Table
      title={<Label variant='headline'>CDC Syncs</Label>}
      toolbar={{
        left: (
          <>
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
            <ReactSelect
              options={sortOptions}
              value={{
                value: sortField,
                label: sortOptions.find((opt) => opt.value === sortField)
                  ?.label,
              }}
              onChange={(val, _) => {
                const sortVal =
                  (val?.value as 'startTime' | 'endTime' | 'numRows') ??
                  'startTime';
                setSortField(sortVal);
              }}
              defaultValue={{ value: 'startTime', label: 'Start Time' }}
            />
          </>
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
            <Label>{row.batchId}</Label>
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
              startTime={row.startTime}
              endTime={row.endTime}
            />
          </TableCell>
          <TableCell>{row.numRows}</TableCell>
        </TableRow>
      ))}
    </Table>
  );
};
