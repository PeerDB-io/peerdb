'use client';

import TimeLabel from '@/components/TimeComponent';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useMemo, useState } from 'react';

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
    return <TimeLabel timeVal={moment(time)?.format('YYYY-MM-DD HH:mm:ss')} />;
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
      <TableCell>
        <Label as='label' style={{ fontSize: 15 }}>
          {partitionId}
        </Label>
      </TableCell>
      <TableCell>
        <Label as='label' style={{ fontSize: 15 }}>
          {runUuid}
        </Label>
      </TableCell>
      <TableCell>
        <Label>{duration}</Label>
      </TableCell>
      <TableCell>
        <TimeLabel timeVal={moment(startTime)?.format('YYYY-MM-DD HH:mm:ss')} />
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

  const [searchQuery, setSearchQuery] = useState<string>('');
  const displayedPartitions = useMemo(
    () =>
      visiblePartitions.filter((partition: QRepPartitionStatus) => {
        return partition.partitionId
          .toLowerCase()
          .includes(searchQuery.toLowerCase());
      }),
    [visiblePartitions, searchQuery]
  );

  const handleNext = () => {
    if (currentPage < totalPages) setCurrentPage(currentPage + 1);
  };

  const handlePrevious = () => {
    if (currentPage > 1) setCurrentPage(currentPage - 1);
  };

  return (
    <Table
      title={<Label>Progress</Label>}
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
            <Button
              variant='normalBorderless'
              onClick={() => window.location.reload()}
            >
              <Icon name='refresh' />
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
        right: (
          <SearchField
            placeholder='Search by partition'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchQuery(e.target.value)
            }
          />
        ),
      }}
      header={
        <TableRow>
          {[
            'Partition UUID',
            'Run UUID',
            'Duration',
            'Start Time',
            'End Time',
            'Num Rows Synced',
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
      {displayedPartitions.map((partition, index) => (
        <RowPerPartition key={index} {...partition} />
      ))}
    </Table>
  );
}
