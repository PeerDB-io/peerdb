'use client';
import SelectTheme from '@/app/styles/select';
import TimeLabel from '@/components/TimeComponent';
import { CloneTableSummary } from '@/grpc_generated/route';
import { Badge } from '@/lib/Badge/Badge';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressBar } from '@/lib/ProgressBar';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useMemo, useState } from 'react';
import ReactSelect from 'react-select';
import { TableCloneSummary } from './snapshot';

const ROWS_PER_PAGE = 5;

function getStatus(clone: CloneTableSummary) {
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
}

export default function SnapshotTable({
  tableLoads,
  title,
}: {
  tableLoads: TableCloneSummary[];
  title: string;
}) {
  const [sortField, setSortField] = useState<
    'cloneStartTime' | 'avgTimePerPartition'
  >('cloneStartTime');

  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.ceil(tableLoads.length / ROWS_PER_PAGE);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('dsc');
  const displayedLoads = useMemo(() => {
    const shownRows = tableLoads.filter((row: TableCloneSummary) =>
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
  }, [tableLoads, currentPage, searchQuery, sortField, sortDir]);

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

  const sortOptions = [
    { value: 'cloneStartTime', label: 'Start Time' },
    { value: 'avgTimePerPartition', label: 'Time Per Partition' },
  ];
  return (
    <Table
      title={<Label variant='headline'>{title}</Label>}
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
                    (val?.value as 'cloneStartTime' | 'avgTimePerPartition') ??
                    'cloneStartTime';
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
          {[
            'Table Identifier',
            'Status',
            'Sync Start',
            'Progress Partitions',
            'Num Rows Processed',
            'Avg Time Per Partition',
          ].map((header) => (
            <TableCell key={header} as='th'>
              {header}
            </TableCell>
          ))}
        </TableRow>
      }
    >
      {displayedLoads.map((clone, index) => (
        <TableRow key={index}>
          <TableCell>
            <Label>{clone.cloneTableSummary.tableName}</Label>
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
              <ProgressBar progress={clone.getPartitionProgressPercentage()} />
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
  );
}
