'use client';

import {
  CDCMirrorStatus,
  QRepMirrorStatus,
  SnapshotStatus,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressBar } from '@/lib/ProgressBar';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import * as Tabs from '@radix-ui/react-tabs';
import moment, { Duration, Moment } from 'moment';
import Link from 'next/link';
import { useState } from 'react';
import styled from 'styled-components';
import CDCDetails from './cdcDetails';

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
const SnapshotStatusTable = ({ status }: SnapshotStatusProps) => (
  <Table
    title={<Label variant='headline'>Initial Copy</Label>}
    toolbar={{
      left: (
        <>
          <Button variant='normalBorderless'>
            <Icon name='chevron_left' />
          </Button>
          <Button variant='normalBorderless'>
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
        </>
      ),
      right: <SearchField placeholder='Search' />,
    }}
    header={
      <TableRow>
        <TableCell as='th' variant='button'>
          <Checkbox variant='mixed' defaultChecked />
        </TableCell>
        <TableCell as='th'>Table Identifier</TableCell>
        <TableCell as='th'>Start Time</TableCell>
        <TableCell as='th'>Progress Partitions</TableCell>
        <TableCell as='th'>Num Rows Synced</TableCell>
        <TableCell as='th'>Avg Time Per Partition</TableCell>
      </TableRow>
    }
  >
    {status.clones.map(summarizeTableClone).map((clone, index) => (
      <TableRow key={index}>
        <TableCell variant='button'>
          <Checkbox />
        </TableCell>
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
          <Label>{clone.cloneStartTime?.format('YYYY-MM-DD HH:mm:ss')}</Label>
        </TableCell>
        <TableCell>
          <ProgressBar progress={clone.getPartitionProgressPercentage()} />
          {clone.completedNumPartitions} / {clone.totalNumPartitions}
        </TableCell>
        <TableCell>{clone.completedNumRows}</TableCell>
        <TableCell>
          <Label>{clone.avgTimePerPartition?.humanize() || 'N/A'}</Label>
        </TableCell>
      </TableRow>
    ))}
  </Table>
);

const Trigger = styled(Tabs.Trigger)<{ isActive?: boolean }>`
  background-color: ${({ theme, isActive }) =>
    isActive ? theme.colors.accent.surface.selected : 'white'};

  font-weight: ${({ isActive }) => (isActive ? 'bold' : 'normal')};

  &:hover {
    color: ${({ theme }) => theme.colors.accent.text.highContrast};
  }
`;

type CDCMirrorStatusProps = {
  cdc: CDCMirrorStatus;
};
export function CDCMirror({ cdc }: CDCMirrorStatusProps) {
  const [selectedTab, setSelectedTab] = useState<string>('tab1');

  let snapshot = <></>;
  if (cdc.snapshotStatus) {
    snapshot = <SnapshotStatusTable status={cdc.snapshotStatus} />;
  }

  return (
    <Tabs.Root
      className='flex flex-col w-full'
      defaultValue={selectedTab}
      onValueChange={setSelectedTab}
    >
      <Tabs.List className='flex border-b' aria-label='Details'>
        <Trigger
          isActive={selectedTab === 'tab1'}
          className='flex-1 px-5 h-[45px] flex items-center justify-center text-sm focus:shadow-outline focus:outline-none'
          value='tab1'
        >
          Details
        </Trigger>
        <Trigger
          isActive={selectedTab === 'tab2'}
          className='flex-1 px-5 h-[45px] flex items-center justify-center text-sm focus:shadow-outline focus:outline-none'
          value='tab2'
        >
          Initial Copy
        </Trigger>
      </Tabs.List>
      <Tabs.Content className='p-5 bg-white rounded-b-md' value='tab1'>
        <CDCDetails config={cdc.config} />
      </Tabs.Content>
      <Tabs.Content className='p-5 bg-white rounded-b-md' value='tab2'>
        {snapshot}
      </Tabs.Content>
    </Tabs.Root>
  );
}
