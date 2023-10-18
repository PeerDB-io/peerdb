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

export type QRepPartitionStatus = {
  partitionId: string;
  runUuid: string;
  status: string;
  startTime: Date | null;
  endTime: Date | null;
  numRows: number | null;
};

function TimeOrProgressBar({ time }: { time: Date | null }) {
  if (time === null) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  } else {
    return <Label>{moment(time)?.format('YYYY-MM-DD HH:mm:ss')}</Label>;
  }
}

function RowPerPartition({
  partitionId,
  runUuid,
  status,
  startTime,
  endTime,
  numRows,
}: QRepPartitionStatus) {
  let duration = 'N/A';
  if (startTime && endTime) {
    duration = moment
      .duration(moment(endTime).diff(moment(startTime)))
      .humanize({ ss: 1 });
  }

  return (
    <TableRow key={partitionId}>
      <TableCell variant='button'>
        <Checkbox />
      </TableCell>
      <TableCell>
        <Label>{partitionId}</Label>
      </TableCell>
      <TableCell>
        <Label>{runUuid}</Label>
      </TableCell>
      <TableCell>
        <Label>{duration}</Label>
      </TableCell>
      <TableCell>
        <Label>{moment(startTime)?.format('YYYY-MM-DD HH:mm:ss')}</Label>
      </TableCell>
      <TableCell>
        <Label>
          <TimeOrProgressBar time={endTime} />
        </Label>
      </TableCell>
      <TableCell>
        <Label>{numRows}</Label>
      </TableCell>
    </TableRow>
  );
}

type QRepStatusTableProps = {
  flowJobName: string;
  partitions: QRepPartitionStatus[];
};

export default function QRepStatusTable({
  flowJobName,
  partitions,
}: QRepStatusTableProps) {
  const ROWS_PER_PAGE = 10;
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.ceil(partitions.length / ROWS_PER_PAGE);

  const visiblePartitions = partitions.slice(
    (currentPage - 1) * ROWS_PER_PAGE,
    currentPage * ROWS_PER_PAGE
  );

  const handleNext = () => {
    if (currentPage < totalPages) setCurrentPage(currentPage + 1);
  };

  const handlePrevious = () => {
    if (currentPage > 1) setCurrentPage(currentPage - 1);
  };

  return (
    <Table
      title={<Label variant='headline'>Progress</Label>}
      toolbar={{
        left: (
          <>
            <Button
              variant='normalBorderless'
              onClick={handlePrevious}
              disabled={currentPage === 1}
            >
              <Icon name='chevron_left' />
            </Button>
            <Button
              variant='normalBorderless'
              onClick={handleNext}
              disabled={currentPage === totalPages}
            >
              <Icon name='chevron_right' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='refresh' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='help' />
            </Button>
            <Button variant='normalBorderless' disabled>
              <Icon name='download' />
            </Button>
            <div>
              <Label>
                {currentPage} of {totalPages}
              </Label>
            </div>
          </>
        ),
        right: <SearchField placeholder='Search' />,
      }}
      header={
        <TableRow>
          <TableCell as='th' variant='button'>
            <Checkbox variant='mixed' defaultChecked />
          </TableCell>
          <TableCell as='th'>Partition UUID</TableCell>
          <TableCell as='th'>Run UUID</TableCell>
          <TableCell as='th'>Duration</TableCell>
          <TableCell as='th'>Start Time</TableCell>
          <TableCell as='th'>End Time</TableCell>
          <TableCell as='th'>Num Rows Synced</TableCell>
        </TableRow>
      }
    >
      {visiblePartitions.map((partition, index) => (
        <RowPerPartition key={index} {...partition} />
      ))}
    </Table>
  );
}
