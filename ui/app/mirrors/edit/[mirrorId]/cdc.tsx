'use client';

import TimeLabel from '@/components/TimeComponent';
import { QRepMirrorStatus, SnapshotStatus } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressBar } from '@/lib/ProgressBar';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import moment, { Duration, Moment } from 'moment';
import Link from 'next/link';
import { useMemo, useState } from 'react';

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
export const SnapshotStatusTable = ({ status }: SnapshotStatusProps) => {
  const [searchQuery, setSearchQuery] = useState<string>('');
  const snapshotRows = useMemo(
    () =>
      status.clones
        .map(summarizeTableClone)
        .filter((row: any) =>
          row.tableName.toLowerCase().includes(searchQuery.toLowerCase())
        ),
    [status.clones, searchQuery]
  );

  return (
    <div style={{ marginTop: '2rem' }}>
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
              <Button
                variant='normalBorderless'
                onClick={() => window.location.reload()}
              >
                <Icon name='refresh' />
              </Button>
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
        {snapshotRows.map((clone, index) => (
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
              <Label>
                {
                  <TimeLabel
                    timeVal={
                      clone.cloneStartTime?.format('YYYY-MM-DD HH:mm:ss') ||
                      'N/A'
                    }
                  />
                }
              </Label>
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
