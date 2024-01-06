import prisma from '@/app/utils/prisma';
import { Label } from '@/lib/Label';
import CdcGraph from './cdcGraph';
import { SyncStatusTable } from './syncStatusTable';

function numberWithCommas(x: Number): string {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
type SyncStatusProps = {
  flowJobName: string | undefined;
  rowsSynced: Number;
};

export default async function SyncStatus({
  flowJobName,
  rowsSynced,
}: SyncStatusProps) {
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
    distinct: ['batch_id'],
  });

  const rows = syncs.map((sync) => ({
    batchId: sync.batch_id,
    startTime: sync.start_time,
    endTime: sync.end_time,
    numRows: sync.rows_in_batch,
  }));

  return (
    <div>
      <div className='flex items-center'>
        <div>
          <Label as='label' style={{ fontSize: 16 }}>
            Rows synced:
          </Label>
        </div>
        <div className='ml-4'>
          <Label style={{ fontSize: 16 }}>{numberWithCommas(rowsSynced)}</Label>
        </div>
      </div>

      <div className='my-10'>
        <CdcGraph syncs={rows} />
      </div>
      <SyncStatusTable rows={rows} />
    </div>
  );
}
