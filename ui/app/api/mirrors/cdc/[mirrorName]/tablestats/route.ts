import {
  MirrorRowsData,
  MirrorTableRowsData,
  UMirrorTableStatsResponse,
} from '@/app/dto/MirrorsDTO';
import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  _: NextRequest,
  context: { params: { mirrorName: string } }
) {
  const mirrorName = context.params.mirrorName;
  let tableSyncs = await prisma.cdc_batch_table.groupBy({
    _sum: {
      insert_count: true,
      update_count: true,
      delete_count: true,
    },
    by: ['destination_table_name'],
    where: {
      flow_name: mirrorName,
    },
  });

  const totalData: MirrorRowsData = {
    totalCount: 0,
    insertsCount: 0,
    updatesCount: 0,
    deletesCount: 0,
  };
  const rowsData: MirrorTableRowsData[] = [];
  tableSyncs.forEach((tableSync) => {
    const inserts = tableSync._sum.insert_count ?? 0;
    const updates = tableSync._sum.update_count ?? 0;
    const deletes = tableSync._sum.delete_count ?? 0;
    const total = inserts + updates + deletes;

    totalData.totalCount += total;
    totalData.insertsCount += inserts;
    totalData.updatesCount += updates;
    totalData.deletesCount += deletes;

    rowsData.push({
      destinationTableName: tableSync.destination_table_name,
      data: {
        totalCount: total,
        insertsCount: inserts,
        updatesCount: updates,
        deletesCount: deletes,
      },
    });
  });

  const res: UMirrorTableStatsResponse = {
    totalData,
    tablesData: rowsData,
  };
  return NextResponse.json(res);
}
