'use client';
import { CloneTableSummary, SnapshotStatus } from '@/grpc_generated/route';
import moment, { Duration, Moment } from 'moment';
import SnapshotTable from './snapshotTable';

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

export function SnapshotStatusTable({ status }: SnapshotStatusProps) {
  const allTableLoads = status.clones.map(summarizeTableClone);
  const completedTableLoads = allTableLoads.filter(
    (row) => row.cloneTableSummary.consolidateCompleted === true
  );
  const inProgressTableLoads = allTableLoads.filter(
    (row) => !row.cloneTableSummary.consolidateCompleted
  );

  return (
    <div
      style={{
        marginTop: '2rem',
        display: 'flex',
        flexDirection: 'column',
        rowGap: '3rem',
      }}
    >
      {[
        { data: inProgressTableLoads, title: 'In progress' },
        { data: completedTableLoads, title: 'Completed tables' },
      ].map((tableLoads, index) => (
        <SnapshotTable
          key={index}
          tableLoads={tableLoads.data}
          title={tableLoads.title}
        />
      ))}
    </div>
  );
}
