'use client';

import TimeLabel from '@/components/TimeComponent';
import {
  CDCMirrorStatus,
  QRepMirrorStatus,
  SnapshotStatus,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressBar } from '@/lib/ProgressBar';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import * as Tabs from '@radix-ui/react-tabs';
import moment, { Duration, Moment } from 'moment';
import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import ReactSelect from 'react-select';
import styled from 'styled-components';
import CdcDetails from './cdcDetails';

class TableCloneSummary {
  flowJobName: string;
  tableName: string;
  totalNumPartitions: number;
  totalNumRows: number;
  completedNumPartitions: number;
  completedNumRows: number;
  avgTimePerPartition: Duration | null;
  cloneStartTime: Moment | null;

  constructor(clone: QRepMirrorStatus) {
    this.flowJobName = clone.config?.flowJobName || '';
    this.tableName = clone.config?.watermarkTable || '';
    this.totalNumPartitions = 0;
    this.totalNumRows = 0;
    this.completedNumPartitions = 0;
    this.completedNumRows = 0;
    this.avgTimePerPartition = null;
    this.cloneStartTime = null;

    this.calculate(clone);
  }

  private calculate(clone: QRepMirrorStatus): void {
    let totalTime = moment.duration(0);
    clone.partitions?.forEach((partition) => {
      this.totalNumPartitions++;
      this.totalNumRows += partition.numRows;

      if (partition.startTime) {
        let st = moment(partition.startTime);
        if (!this.cloneStartTime || st.isBefore(this.cloneStartTime)) {
          this.cloneStartTime = st;
        }
      }

      if (partition.endTime) {
        this.completedNumPartitions++;
        this.completedNumRows += partition.numRows;
        let st = moment(partition.startTime);
        let et = moment(partition.endTime);
        let duration = moment.duration(et.diff(st));
        totalTime = totalTime.add(duration);
      }
    });

    if (this.completedNumPartitions > 0) {
      this.avgTimePerPartition = moment.duration(
        totalTime.asMilliseconds() / this.completedNumPartitions
      );
    }
  }

  getRowProgressPercentage(): number {
    if (this.totalNumRows === 0) {
      return 0;
    }
    return (this.completedNumRows / this.totalNumRows) * 100;
  }

  getPartitionProgressPercentage(): number {
    if (this.totalNumPartitions === 0) {
      return 0;
    }
    return (this.completedNumPartitions / this.totalNumPartitions) * 100;
  }
}

function summarizeTableClone(clone: QRepMirrorStatus): TableCloneSummary {
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
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('asc');
  const displayedRows = useMemo(() => {
    const shownRows = allRows.filter((row: any) =>
      row.tableName.toLowerCase().includes(searchQuery.toLowerCase())
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

  const sortOptions = [
    { value: 'cloneStartTime', label: 'Start Time' },
    { value: 'avgTimePerPartition', label: 'Time Per Partition' },
  ];
  return (
    <div style={{ marginTop: '2rem' }}>
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
              <Button
                variant='normalBorderless'
                onClick={() => window.location.reload()}
              >
                <Icon name='refresh' />
              </Button>
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
                defaultValue={{ value: 'cloneStartTime', label: 'Start Time' }}
              />
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
            </>
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
            <TableCell as='th'>Start Time</TableCell>
            <TableCell as='th'>Progress Partitions</TableCell>
            <TableCell as='th'>Num Rows Synced</TableCell>
            <TableCell as='th'>Avg Time Per Partition</TableCell>
          </TableRow>
        }
      >
        {displayedRows.map((clone, index) => (
          <TableRow key={index}>
            <TableCell>
              <Label>
                <Link
                  href={`/mirrors/status/qrep/${clone.flowJobName}`}
                  className='underline cursor-pointer'
                >
                  {clone.tableName}
                </Link>
              </Label>
            </TableCell>
            <TableCell>
              <TimeLabel
                timeVal={
                  clone.cloneStartTime?.format('YYYY-MM-DD HH:mm:ss') || 'N/A'
                }
              />
            </TableCell>
            <TableCell>
              <ProgressBar progress={clone.getPartitionProgressPercentage()} />
              {clone.completedNumPartitions} / {clone.totalNumPartitions}
            </TableCell>
            <TableCell>{clone.completedNumRows}</TableCell>
            <TableCell>
              <Label>
                {clone.avgTimePerPartition?.humanize({ ss: 1 }) || 'N/A'}
              </Label>
            </TableCell>
          </TableRow>
        ))}
      </Table>
    </div>
  );
};

const Trigger = styled(
  ({ isActive, ...props }: { isActive?: boolean } & Tabs.TabsTriggerProps) => (
    <Tabs.Trigger {...props} />
  )
)<{ isActive?: boolean }>`
  background-color: ${({ theme, isActive }) =>
    isActive ? theme.colors.accent.surface.selected : 'white'};

  font-weight: ${({ isActive }) => (isActive ? 'bold' : 'normal')};

  &:hover {
    color: ${({ theme }) => theme.colors.accent.text.highContrast};
  }
`;

type SyncStatusRow = {
  batchId: number;
  startTime: Date;
  endTime: Date | null;
  numRows: number;
};

type CDCMirrorStatusProps = {
  cdc: CDCMirrorStatus;
  rows: SyncStatusRow[];
  createdAt?: Date;
  syncStatusChild?: React.ReactNode;
};
export function CDCMirror({
  cdc,
  rows,
  createdAt,
  syncStatusChild,
}: CDCMirrorStatusProps) {
  const [selectedTab, setSelectedTab] = useState('');

  let snapshot = <></>;
  if (cdc.snapshotStatus) {
    snapshot = <SnapshotStatusTable status={cdc.snapshotStatus} />;
  }

  const handleTab = (tabVal: string) => {
    localStorage.setItem('mirrortab', tabVal);
    setSelectedTab(tabVal);
  };

  useEffect(() => {
    if (typeof window !== 'undefined') {
      setSelectedTab(localStorage?.getItem('mirrortab') || 'tab1');
    }
  }, []);

  return (
    <Tabs.Root
      className='flex flex-col w-full'
      value={selectedTab}
      onValueChange={(val) => handleTab(val)}
      style={{ marginTop: '2rem' }}
    >
      <Tabs.List className='flex border-b' aria-label='Details'>
        <Trigger
          isActive={selectedTab === 'tab1'}
          className='flex-1 px-5 h-[45px] flex items-center justify-center text-sm focus:shadow-outline focus:outline-none'
          value='tab1'
        >
          Overview
        </Trigger>
        <Trigger
          isActive={selectedTab === 'tab2'}
          className='flex-1 px-5 h-[45px] flex items-center justify-center text-sm focus:shadow-outline focus:outline-none'
          value='tab2'
        >
          Sync Status
        </Trigger>
        <Trigger
          isActive={selectedTab === 'tab3'}
          className='flex-1 px-5 h-[45px] flex items-center justify-center text-sm focus:shadow-outline focus:outline-none'
          value='tab3'
        >
          Initial Copy
        </Trigger>
      </Tabs.List>
      <Tabs.Content className='p-5 rounded-b-md' value='tab1'>
        <CdcDetails
          syncs={rows}
          createdAt={createdAt}
          mirrorConfig={cdc.config}
        />
      </Tabs.Content>
      <Tabs.Content className='p-5 rounded-b-md' value='tab2'>
        {syncStatusChild}
      </Tabs.Content>
      <Tabs.Content className='p-5 rounded-b-md' value='tab3'>
        {snapshot}
      </Tabs.Content>
    </Tabs.Root>
  );
}
