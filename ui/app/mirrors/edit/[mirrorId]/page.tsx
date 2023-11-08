import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Header } from '@/lib/Header';
import { LayoutMain } from '@/lib/Layout';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { redirect } from 'next/navigation';
import { Suspense } from 'react';
import CdcDetails from './cdcDetails';
import SyncStatus from './syncStatus';
import prisma from '@/app/utils/prisma';


export const dynamic = 'force-dynamic';

type EditMirrorProps = {
  params: { mirrorId: string };
};

function getMirrorStatusUrl(mirrorId: string) {
  let base = GetFlowHttpAddressFromEnv();
  return `${base}/v1/mirrors/${mirrorId}`;
}

async function getMirrorStatus(mirrorId: string) {
  const url = getMirrorStatusUrl(mirrorId);
  const resp = await fetch(url);
  const json = await resp.json();
  return json;
}

function Loading() {
  return <div>Loading...</div>;
}

export default async function EditMirror({
  params: { mirrorId },
}: EditMirrorProps) {
  const mirrorStatus: MirrorStatusResponse = await getMirrorStatus(mirrorId);
  if (!mirrorStatus) {
    return <div>No mirror status found!</div>;
  }

  let syncStatusChild = <></>;
  if (mirrorStatus.cdcStatus) {
    syncStatusChild = <SyncStatus flowJobName={mirrorId} />;
  } else {
    redirect(`/mirrors/status/qrep/${mirrorId}`);
  }

  let syncs = await prisma.cdc_batches.findMany({
    where: {
      flow_name: mirrorId,
      start_time: {
        not: undefined,
      },
    },
    orderBy: {
      start_time: 'desc',
    },
  })
  
  const rows = syncs.map((sync) => ({
    batchId: sync.id,
    startTime: sync.start_time,
    endTime: sync.end_time,
    numRows: sync.rows_in_batch,
  }));

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Header variant='title2'>{mirrorId}</Header>
      <Suspense fallback={<Loading />}>
        {mirrorStatus.cdcStatus && (
          <CdcDetails syncs={rows} mirrorConfig={mirrorStatus.cdcStatus.config} />
        )}
        <div className='mt-10'>
        {syncStatusChild}
        </div>
      </Suspense>
    </LayoutMain>
  );
}