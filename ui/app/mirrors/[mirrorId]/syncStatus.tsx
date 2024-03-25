import { MirrorRowsData, SyncStatusRow } from '@/app/dto/MirrorsDTO';
import prisma from '@/app/utils/prisma';
import CdcGraph from './cdcGraph';
import RowsDisplay from './rowsDisplay';
import { SyncStatusTable } from './syncStatusTable';
import TableStats from './tableStats';

type SyncStatusProps = {
  flowJobName: string | undefined;
  rowsSynced: Number;
  rows: SyncStatusRow[];
};

const GetTablesStats = async (flowName: string) => {
  let tableSyncs = await prisma.cdc_batch_table.groupBy({
    _sum: {
      insert_count: true,
      update_count: true,
      delete_count: true,
    },
    by: ['destination_table_name'],
    where: {
      flow_name: flowName,
    },
  });

  const rowsData: MirrorRowsData[] = [];
  tableSyncs.forEach((tableSync) => {
    const inserts = tableSync._sum.insert_count ?? 0;
    const updates = tableSync._sum.update_count ?? 0;
    const deletes = tableSync._sum.delete_count ?? 0;
    const total = inserts + updates + deletes;
    rowsData.push({
      destinationTableName: tableSync.destination_table_name,
      insertCount: inserts,
      updateCount: updates,
      deleteCount: deletes,
      totalCount: total,
    });
  });
  return rowsData;
};

export default async function SyncStatus({
  flowJobName,
  rowsSynced,
  rows,
}: SyncStatusProps) {
  if (!flowJobName) {
    return <div>Flow job name not provided!</div>;
  }

  const tableSyncs = await GetTablesStats(flowJobName);
  const inserts = tableSyncs.reduce(
    (acc, tableData) => acc + tableData.insertCount,
    0
  );
  const updates = tableSyncs.reduce(
    (acc, tableData) => acc + tableData.updateCount,
    0
  );
  const deletes = tableSyncs.reduce(
    (acc, tableData) => acc + tableData.deleteCount,
    0
  );
  const totalRowsData = {
    total: inserts + updates + deletes,
    inserts,
    updates,
    deletes,
  };
  return (
    <div>
      <RowsDisplay totalRowsData={totalRowsData} />
      <div className='my-10'>
        <CdcGraph syncs={rows} />
      </div>
      <SyncStatusTable rows={rows} />
      <TableStats tableSyncs={tableSyncs} />
    </div>
  );
}
