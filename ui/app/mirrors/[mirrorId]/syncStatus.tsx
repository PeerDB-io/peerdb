'use client';
import { fetcher } from '@/app/utils/swr';
import { CDCTableTotalCountsResponse } from '@/grpc_generated/route';
import useSWR from 'swr';
import CdcGraph from './cdcGraph';
import LagGraph from './lagGraph';
import RowsDisplay from './rowsDisplay';
import { SyncStatusTable } from './syncStatusTable';
import TableStats from './tableStats';

type SyncStatusProps = {
  flowJobName: string;
};

export default function SyncStatus({ flowJobName }: SyncStatusProps) {
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
        <div
          style={{
            marginBlock: '2.5rem',
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(480px, 1fr))',
            gap: '2rem',
          }}
        >
          <div style={{ minWidth: 0 }}>
            <CdcGraph mirrorName={flowJobName} />
          </div>
          <div style={{ minWidth: 0 }}>
            <LagGraph mirrorName={flowJobName} />
          </div>
        </div>
        <SyncStatusTable mirrorName={flowJobName} />
        <TableStats tableSyncs={tableStats.tablesData} />
      </div>
    )
  );
}
