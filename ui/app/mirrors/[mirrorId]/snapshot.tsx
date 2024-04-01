'use client';
import SelectTheme from '@/app/styles/select';
import TimeLabel from '@/components/TimeComponent';
import { CloneTableSummary, SnapshotStatus } from '@/grpc_generated/route';
import { Badge } from '@/lib/Badge/Badge';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressBar } from '@/lib/ProgressBar';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment, { Duration, Moment } from 'moment';
import Link from 'next/link';
import { useMemo, useState } from 'react';
import ReactSelect from 'react-select';

export class TableCloneSummary {
  cloneStartTime: Moment | null = null;
  cloneTableSummary: CloneTableSummary;
  avgTimePerPartition: Duration | null = null;
  constructor(clone: CloneTableSummary) {
    this.cloneTableSummary = clone;
    if (clone.startTime) {
      this.cloneStartTime = moment(clone.startTime);
    }
    if (clone.avgTimePerPartitionMs) {
      this.avgTimePerPartition = moment.duration(
        clone.avgTimePerPartitionMs,
        'ms'
      );
    }
  }

  getPartitionProgressPercentage(): number {
    if (this.cloneTableSummary.numPartitionsTotal === 0) {
      return 0;
    }
    return (
      (this.cloneTableSummary.numPartitionsCompleted /
        this.cloneTableSummary.numPartitionsTotal) *
      100
    );
  }
}

export function summarizeTableClone(
  clone: CloneTableSummary
): TableCloneSummary {
  return new TableCloneSummary(clone);
}

type SnapshotStatusProps = {
  status: SnapshotStatus;
};

const ROWS_PER_PAGE = 5;
export const SnapshotStatusTable = ({ status }: SnapshotStatusProps) => {
  const [sortField, setSortField] = useState<
    'cloneStartTime' | 'avgTimePerPartition'
  >('cloneStartTime');
  const allRows = status.clones.map(summarizeTableClone);
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.ceil(allRows.length / ROWS_PER_PAGE);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('dsc');
  const displayedRows = useMemo(() => {
    const shownRows = allRows.filter((row: TableCloneSummary) =>
      row.cloneTableSummary.tableName
        ?.toLowerCase()
        .includes(searchQuery.toLowerCase())
    );
    shownRows.sort((a, b) => {
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

    const startRow = (currentPage - 1) * ROWS_PER_PAGE;
    const endRow = startRow + ROWS_PER_PAGE;
    return shownRows.length > ROWS_PER_PAGE
      ? shownRows.slice(startRow, endRow)
      : shownRows;
  }, [allRows, currentPage, searchQuery, sortField, sortDir]);

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

  const getStatus = (clone: CloneTableSummary) => {
    if (clone.consolidateCompleted) {
      return (
        <Badge type='longText' variant='positive'>
          <Icon name='check' />
          <div className='font-bold'>Done</div>
        </Badge>
      );
    }
    if (!clone.fetchCompleted) {
      return (
        <Badge type='longText' variant='normal'>
          <Icon name='download' />
          <div className='font-bold'>Fetching</div>
        </Badge>
      );
    }

    if (clone.numPartitionsCompleted == clone.numPartitionsTotal) {
      return (
        <Badge type='longText' variant='normal'>
          <Icon name='upload' />
          <div className='font-bold'>Consolidating</div>
        </Badge>
      );
    }

    return (
      <Badge type='longText' variant='normal'>
        <Icon name='upload' />
        <div className='font-bold'>Syncing</div>
      </Badge>
    );
  };

  const sortOptions = [
    { value: 'cloneStartTime', label: 'Start Time' },
    { value: 'avgTimePerPartition', label: 'Time Per Partition' },
  ];
  return (
    <div
      style={{
        marginTop: '2rem',
        display: 'flex',
        flexDirection: 'column',
        rowGap: '1rem',
      }}
    >
      <Table
        title={<Label variant='headline'>Initial Copy</Label>}
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
                  onChange={(val, _) => {
                    const sortVal =
                      (val?.value as
                        | 'cloneStartTime'
                        | 'avgTimePerPartition') ?? 'cloneStartTime';
                    setSortField(sortVal);
                  }}
                  value={{
                    value: sortField,
                    label: sortOptions.find((opt) => opt.value === sortField)
                      ?.label,
                  }}
                  defaultValue={{
                    value: 'cloneStartTime',
                    label: 'Start Time',
                  }}
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
              placeholder='Search by table name'
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setSearchQuery(e.target.value)
              }
            />
          ),
        }}
        header={
          <TableRow>
            <TableCell as='th'>Table Identifier</TableCell>
            <TableCell as='th'>Status</TableCell>
            <TableCell as='th'>Sync Start</TableCell>
            <TableCell as='th'>Progress Partitions</TableCell>
            <TableCell as='th'>Num Rows Processed</TableCell>
            <TableCell as='th'>Avg Time Per Partition</TableCell>
          </TableRow>
        }
      >
        {displayedRows.map((clone, index) => (
          <TableRow key={index}>
            <TableCell>
              <Label>
                <Link
                  href={`/mirrors/status/qrep/${clone.cloneTableSummary.flowJobName}`}
                  className='underline cursor-pointer'
                >
                  {clone.cloneTableSummary.tableName}
                </Link>
              </Label>
            </TableCell>
            <TableCell>{getStatus(clone.cloneTableSummary)}</TableCell>
            <TableCell>
              {clone.cloneStartTime ? (
                <TimeLabel
                  timeVal={clone.cloneStartTime.format('YYYY-MM-DD HH:mm:ss')}
                />
              ) : (
                'N/A'
              )}
            </TableCell>
            {clone.cloneTableSummary.fetchCompleted ? (
              <TableCell>
                <ProgressBar
                  progress={clone.getPartitionProgressPercentage()}
                />
                {clone.cloneTableSummary.numPartitionsCompleted} /{' '}
                {clone.cloneTableSummary.numPartitionsTotal}
              </TableCell>
            ) : (
              <TableCell>N/A</TableCell>
            )}
            <TableCell>
              {clone.cloneTableSummary.fetchCompleted
                ? clone.cloneTableSummary.numRowsSynced
                : 0}
            </TableCell>
            {clone.cloneTableSummary.fetchCompleted ? (
              <TableCell>
                <Label>
                  {clone.avgTimePerPartition?.humanize({ ss: 1 }) || 'N/A'}
                </Label>
              </TableCell>
            ) : (
              <TableCell>N/A</TableCell>
            )}
          </TableRow>
        ))}
      </Table>
    </div>
  );
};
