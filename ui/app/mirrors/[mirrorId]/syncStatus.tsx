import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import { Label } from '@/lib/Label';
import CdcGraph from './cdcGraph';
import { SyncStatusTable } from './syncStatusTable';

function numberWithCommas(x: Number): string {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
type SyncStatusProps = {
  flowJobName: string | undefined;
  rowsSynced: Number;
  rows: SyncStatusRow[];
};

export default async function SyncStatus({
  flowJobName,
  rowsSynced,
  rows,
}: SyncStatusProps) {
  if (!flowJobName) {
    return <div>Flow job name not provided!</div>;
  }

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
