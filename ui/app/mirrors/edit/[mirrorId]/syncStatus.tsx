import prisma from '@/app/utils/prisma';
import { SyncStatusTable } from './syncStatusTable';

type SyncStatusProps = {
  flowJobName: string | undefined;
};

export default async function SyncStatus({ flowJobName }: SyncStatusProps) {
  if (!flowJobName) {
    return <div>Flow job name not provided!</div>;
  }

  const syncs = await prisma.cdc_batches.findMany({
    where: {
      flow_name: flowJobName,
      start_time: {
        not: undefined,
      },
    },
    orderBy: {
      start_time: 'desc',
    },
  });

  const rows = syncs.map((sync) => ({
    batchId: sync.id,
    startTime: sync.start_time,
    endTime: sync.end_time,
    numRows: sync.rows_in_batch,
  }));

  return <div>
    <SyncStatusTable rows={rows} />
  </div>;
}
