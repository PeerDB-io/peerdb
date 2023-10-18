'use client';

import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useState } from 'react';

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
      <Label>
        {moment(endTime).format('YYYY-MM-DD HH:mm:ss')} (
        {moment.duration(moment(endTime).diff(startTime)).humanize({ ss: 1 })})
      </Label>
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

export const SyncStatusTable = ({ rows }: SyncStatusTableProps) => {
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.ceil(rows.length / ROWS_PER_PAGE);

  const startRow = (currentPage - 1) * ROWS_PER_PAGE;
  const endRow = startRow + ROWS_PER_PAGE;

  const displayedRows = rows.slice(startRow, endRow);

  const handlePrevPage = () => {
    if (currentPage > 1) setCurrentPage(currentPage - 1);
  };

  const handleNextPage = () => {
    if (currentPage < totalPages) setCurrentPage(currentPage + 1);
  };

  return (
    <Table
      title={<Label variant='headline'>Initial Copy</Label>}
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
            <Button variant='normalBorderless'>
              <Icon name='refresh' />
            </Button>
          </>
        ),
        right: <SearchField placeholder='Search' />,
      }}
      header={
        <TableRow>
          <TableCell as='th' variant='button'>
            <Checkbox variant='mixed' defaultChecked />
          </TableCell>
          <TableCell as='th'>Batch ID</TableCell>
          <TableCell as='th'>Start Time</TableCell>
          <TableCell as='th'>End Time (Duation)</TableCell>
          <TableCell as='th'>Num Rows Synced</TableCell>
        </TableRow>
      }
    >
      {displayedRows.map((row, index) => (
        <TableRow key={index}>
          <TableCell variant='button'>
            <Checkbox />
          </TableCell>
          <TableCell>
            <Label>{row.batchId}</Label>
          </TableCell>
          <TableCell>
            <Label>{moment(row.startTime).format('YYYY-MM-DD HH:mm:ss')}</Label>
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
