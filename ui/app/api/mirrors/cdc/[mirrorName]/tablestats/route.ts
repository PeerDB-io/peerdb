import {
  MirrorRowsData,
  MirrorTableRowsData,
  UMirrorTableStatsResponse,
} from '@/app/dto/MirrorsDTO';
import { CDCBatchTotalsResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  _: NextRequest,
  context: { params: { mirrorName: string } }
) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const tableSyncsReq = await fetch(
    `${flowServiceAddr}/v1/cdcbatchtotals/${encodeURIComponent(context.params.mirrorName)}`
  );
  const tableSyncs: CDCBatchTotalsResponse = await tableSyncsReq.json();

  const totalData: MirrorRowsData = {
    totalCount: 0,
    insertsCount: 0,
    updatesCount: 0,
    deletesCount: 0,
  };
  const rowsData: MirrorTableRowsData[] = tableSyncs.totals.map((tableSync) => {
    const { tableName, inserts, updates, deletes } = tableSync;
    const total = inserts + updates + deletes;

    totalData.totalCount += total;
    totalData.insertsCount += inserts;
    totalData.updatesCount += updates;
    totalData.deletesCount += deletes;

    return {
      destinationTableName: tableName,
      data: {
        totalCount: total,
        insertsCount: inserts,
        updatesCount: updates,
        deletesCount: deletes,
      },
    };
  });

  const res: UMirrorTableStatsResponse = {
    totalData,
    tablesData: rowsData,
  };
  return NextResponse.json(res);
}
