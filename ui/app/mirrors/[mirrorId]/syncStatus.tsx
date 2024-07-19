'use client';
import { fetcher } from '@/app/utils/swr';
import { CDCBatch, CDCTableTotalCountsResponse } from '@/grpc_generated/route';
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
  const {
    data: tableStats,
    error,
    isLoading,
  } = useSWR<CDCTableTotalCountsResponse>(
    `/api/v1/mirrors/cdc/table_total_counts/${encodeURIComponent(flowJobName)}`,
    fetcher
  );

  return (
    !isLoading &&
    !error &&
    tableStats &&
    tableStats?.totalData &&
    tableStats?.tablesData && (
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
