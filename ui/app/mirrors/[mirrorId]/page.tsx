import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import prisma from '@/app/utils/prisma';
import EditButton from '@/components/EditButton';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Header } from '@/lib/Header';
import { LayoutMain } from '@/lib/Layout';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { redirect } from 'next/navigation';
import { CDCMirror } from './cdc';
import NoMirror from './nomirror';
import SyncStatus from './syncStatus';

type EditMirrorProps = {
  params: { mirrorId: string };
};

function getMirrorStatusUrl(mirrorId: string) {
  let base = GetFlowHttpAddressFromEnv();
  return `${base}/v1/mirrors/${mirrorId}`;
}

async function getMirrorStatus(mirrorId: string) {
  const url = getMirrorStatusUrl(mirrorId);
  const resp = await fetch(url, { cache: 'no-store' });
  const json = await resp.json();
  return json;
}

export default async function EditMirror({
  params: { mirrorId },
}: EditMirrorProps) {
  const mirrorStatus: MirrorStatusResponse = await getMirrorStatus(mirrorId);
  if (!mirrorStatus) {
    return <div>No mirror status found!</div>;
  }

  let mirrorInfo = await prisma.flows.findFirst({
    select: {
      created_at: true,
      workflow_id: true,
    },
    where: {
      name: mirrorId,
    },
  });

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
    distinct: ['batch_id'],
  });

  const rows: SyncStatusRow[] = syncs.map((sync) => ({
    batchId: sync.batch_id,
    startTime: sync.start_time,
    endTime: sync.end_time,
    numRows: sync.rows_in_batch,
  }));

  if (mirrorStatus.errorMessage) {
    return <NoMirror />;
  }

  let syncStatusChild = <></>;
  if (mirrorStatus.cdcStatus) {
    let rowsSynced = syncs.reduce((acc, sync) => {
      if (sync.end_time !== null) {
        return acc + sync.rows_in_batch;
      }
      return acc;
    }, 0);
    syncStatusChild = (
      <SyncStatus rowsSynced={rowsSynced} rows={rows} flowJobName={mirrorId} />
    );
  } else {
    redirect(`/mirrors/status/qrep/${mirrorId}`);
  }

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <Header variant='title2'>{mirrorId}</Header>
        <EditButton toLink={`/mirrors/${mirrorId}/edit`} />
      </div>
      <CDCMirror
        rows={rows}
        createdAt={mirrorInfo?.created_at}
        syncStatusChild={syncStatusChild}
        status={mirrorStatus}
      />
    </LayoutMain>
  );
}
