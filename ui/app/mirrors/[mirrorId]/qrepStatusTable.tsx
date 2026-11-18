'use client';

import { useSelectTheme } from '@/app/styles/select';
import TimeLabel from '@/components/TimeComponent';
import { PartitionStatus } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { useSortButtonColor } from '@/lib/hooks/useSortButtonColor';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment from 'moment';
import { useMemo, useState } from 'react';
import ReactSelect from 'react-select';

function TimeOrProgressBar({ time }: { time: Date | null }) {
  if (time === null) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  } else {
    return <TimeLabel timeVal={moment(time)?.format('YYYY-MM-DD HH:mm:ss')} />;
  }
}

function RowPerPartition({
  partitionId,
  startTime,
  endTime,
  rowsInPartition,
  rowsSynced,
}: PartitionStatus) {
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
        <Label>{duration}</Label>
      </TableCell>
      <TableCell>
        <TimeLabel timeVal={moment(startTime)?.format('YYYY-MM-DD HH:mm:ss')} />
      </TableCell>
      <TableCell>
        <TimeOrProgressBar time={endTime || null} />
      </TableCell>
      <TableCell>
        <Label>{rowsInPartition}</Label>
      </TableCell>
      <TableCell>
        <Label>{rowsSynced ?? 0}</Label>
      </TableCell>
    </TableRow>
  );
}

type QRepStatusTableProps = {
  flowJobName: string;
  partitions: PartitionStatus[];
};

export default function QRepStatusTable({ partitions }: QRepStatusTableProps) {
  const selectTheme = useSelectTheme();
  const sortButtonColor = useSortButtonColor();
  const ROWS_PER_PAGE = 10;
  const [currentPage, setCurrentPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [sortField, setSortField] = useState<'startTime' | 'endTime'>(
    'startTime'
  );
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('dsc');

  // filter -> sort -> paginate, so sorting and search apply to every partition
  // and not just the rows that happen to be on the current page
  const sortedPartitions = useMemo(() => {
    const filtered = partitions.filter((partition: PartitionStatus) => {
      return partition.partitionId
        .toLowerCase()
        .includes(searchQuery.toLowerCase());
    });
    filtered.sort((a, b) => {
      // maximum date values so compare towards the end
      const aValue = a[sortField] ?? new Date(8.64e15);
      const bValue = b[sortField] ?? new Date(8.64e15);

      if (aValue < bValue) {
        return sortDir === 'dsc' ? 1 : -1;
      } else if (aValue > bValue) {
        return sortDir === 'dsc' ? -1 : 1;
      } else {
        return 0;
      }
    });
    return filtered;
  }, [partitions, searchQuery, sortField, sortDir]);

  const totalPages = Math.max(
    1,
    Math.ceil(sortedPartitions.length / ROWS_PER_PAGE)
  );
  const page = Math.min(currentPage, totalPages);
  const displayedPartitions = sortedPartitions.slice(
    (page - 1) * ROWS_PER_PAGE,
    page * ROWS_PER_PAGE
  );

  const handleNext = () => {
    if (page < totalPages) setCurrentPage(page + 1);
  };

  const handlePrevious = () => {
    if (page > 1) setCurrentPage(page - 1);
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
              disabled={page === 1}
            >
              <Icon name='chevron_left' />
            </Button>
            <Button
              variant='normalBorderless'
              onClick={handleNext}
              disabled={page === totalPages}
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
                theme={selectTheme}
              />
            </div>
            <button
              className='IconButton'
              onClick={() => setSortDir('asc')}
              aria-label='sort up'
              style={{ color: sortButtonColor(sortDir === 'asc') }}
            >
              <Icon name='arrow_upward' />
            </button>
            <button
              className='IconButton'
              onClick={() => setSortDir('dsc')}
              aria-label='sort down'
              style={{ color: sortButtonColor(sortDir === 'dsc') }}
            >
              <Icon name='arrow_downward' />
            </button>
            <div>
              <Label>
                {page} of {totalPages}
              </Label>
            </div>
          </div>
        ),
        right: (
          <SearchField
            placeholder='Search by partition'
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              setSearchQuery(e.target.value);
              setCurrentPage(1);
            }}
          />
        ),
      }}
      header={
        <TableRow>
          {[
            'Partition UUID',
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
