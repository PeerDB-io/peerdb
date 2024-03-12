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
import ReactSelect from 'react-select';

export type QRepPartitionStatus = {
  partitionId: string;
  runUuid: string;
  status: string;
  startTime: Date | null;
  endTime: Date | null;
  pulledRows: number | null;
  syncedRows: number | null;
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
  pulledRows: numRows,
  syncedRows,
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
        <TimeOrProgressBar time={endTime} />
      </TableCell>
      <TableCell>
        <Label>{numRows}</Label>
      </TableCell>
      <TableCell>
        <Label>{syncedRows ?? 0}</Label>
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
  const [sortField, setSortField] = useState<'startTime' | 'endTime'>(
    'startTime'
  );
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('dsc');
  const displayedPartitions = useMemo(() => {
    const currentPartitions = visiblePartitions.filter(
      (partition: QRepPartitionStatus) => {
        return partition.partitionId
          .toLowerCase()
          .includes(searchQuery.toLowerCase());
      }
    );
    currentPartitions.sort((a, b) => {
      const aValue = a[sortField];
      const bValue = b[sortField];
      if (aValue === null || bValue === null) {
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
    return currentPartitions;
  }, [visiblePartitions, searchQuery, sortField, sortDir]);

  const handleNext = () => {
    if (currentPage < totalPages) setCurrentPage(currentPage + 1);
  };

  const handlePrevious = () => {
    if (currentPage > 1) setCurrentPage(currentPage - 1);
  };

  const sortOptions = [
    { value: 'startTime', label: 'Start Time' },
    { value: 'endTime', label: 'End Time' },
  ];
  return (
    <Table
      title={<Label>Progress</Label>}
      toolbar={{
        left: (
          <div style={{ display: 'flex', alignItems: 'center' }}>
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
                    (val?.value as 'startTime' | 'endTime') ?? 'startTime';
                  setSortField(sortVal);
                }}
                defaultValue={{ value: 'startTime', label: 'Start Time' }}
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
            <div>
              <Label>
                {currentPage} of {totalPages}
              </Label>
            </div>
          </div>
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
            'Rows In Partition',
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
      {displayedPartitions.map((partition, index) => (
        <RowPerPartition key={index} {...partition} />
      ))}
    </Table>
  );
}
