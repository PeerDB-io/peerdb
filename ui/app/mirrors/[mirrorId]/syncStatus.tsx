'use client';
import { UMirrorTableStatsResponse } from '@/app/dto/MirrorsDTO';
import { fetcher } from '@/app/utils/swr';
import { CDCBatch } from '@/grpc_generated/route';
import useSWR from 'swr';
import CdcGraph from './cdcGraph';
import RowsDisplay from './rowsDisplay';
import { SyncStatusTable } from './syncStatusTable';
import TableStats from './tableStats';

type SyncStatusProps = {
  flowJobName: string;
  rows: CDCBatch[];
};

export default function SyncStatus({ flowJobName, rows }: SyncStatusProps) {
  const { data: tableStats, isLoading } = useSWR<UMirrorTableStatsResponse>(
    `/api/mirrors/cdc/${flowJobName}/tablestats`,
    fetcher
  );

  return (
    !isLoading &&
    tableStats && (
      <div>
        <RowsDisplay totalRowsData={tableStats.totalData} />
        <div className='my-10'>
          <CdcGraph syncs={rows} />
        </div>
        <SyncStatusTable rows={rows} />
        <TableStats tableSyncs={tableStats.tablesData} />
      </div>
    )
  );
}
