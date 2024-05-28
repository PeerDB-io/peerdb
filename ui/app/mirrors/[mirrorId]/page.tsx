import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import { GetHashedPeerDBPasswordFromEnv } from '@/app/utils/passwordFromEnv';
import prisma from '@/app/utils/prisma';
import MirrorActions from '@/components/MirrorActionsDropdown';
import { FlowConnectionConfigs, FlowStatus } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
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
  return `${base}/v1/mirrors/${mirrorId}?include_flow_info=true`;
}

async function getMirrorStatus(mirrorId: string) {
  const url = getMirrorStatusUrl(mirrorId);
  const hashedPassword = GetHashedPeerDBPasswordFromEnv();
  const resp = await fetch(url, {
    cache: 'no-store',
    headers: { Authorization: `Bearer ${hashedPassword}` },
  });
  const json = await resp.json();
  return json;
}

export default async function ViewMirror({
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
      config_proto: true,
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

  if (!mirrorInfo) {
    return <div>No mirror info found</div>;
  }

  let syncStatusChild = null;
  let actionsDropdown = null;

  if (mirrorStatus.cdcStatus) {
    let rowsSynced = syncs.reduce((acc, sync) => {
      if (sync.end_time !== null) {
        return acc + sync.rows_in_batch;
      }
      return acc;
    }, 0);
    const mirrorConfig = FlowConnectionConfigs.decode(mirrorInfo.config_proto!);
    syncStatusChild = (
      <SyncStatus rowsSynced={rowsSynced} rows={rows} flowJobName={mirrorId} />
    );

    const dbType = mirrorConfig.destination!.type;

    const isNotPaused =
      mirrorStatus.currentFlowState.toString() !==
      FlowStatus[FlowStatus.STATUS_PAUSED];
    const canResync =
      mirrorStatus.currentFlowState.toString() !==
        FlowStatus[FlowStatus.STATUS_SETUP] &&
      (dbType.valueOf() === DBType.BIGQUERY.valueOf() ||
        dbType.valueOf() === DBType.SNOWFLAKE.valueOf());

    actionsDropdown = (
      <MirrorActions
        mirrorConfig={mirrorConfig}
        workflowId={mirrorInfo.workflow_id || ''}
        editLink={`/mirrors/${mirrorId}/edit`}
        canResync={canResync}
        isNotPaused={isNotPaused}
      />
    );
  } else {
    redirect(`/mirrors/status/qrep/${mirrorId}`);
  }

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          paddingRight: '2rem',
        }}
      >
        <Header variant='title2'>{mirrorId}</Header>
        {actionsDropdown}
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
