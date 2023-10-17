import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Header } from '@/lib/Header';
import { LayoutMain } from '@/lib/Layout';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { Suspense } from 'react';
import { CDCMirror } from './cdc';
import SyncStatus from './syncStatus';

type EditMirrorProps = {
  params: { mirrorId: string };
};

function getMirrorStatusUrl(mirrorId: string) {
  let base = GetFlowHttpAddressFromEnv();
  return `${base}/v1/mirrors/${mirrorId}`;
}

export async function getMirrorStatus(mirrorId: string) {
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
  }

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Header variant='title2'>{mirrorId}</Header>
      <Suspense fallback={<Loading />}>
        {mirrorStatus.cdcStatus && (
          <CDCMirror
            cdc={mirrorStatus.cdcStatus}
            syncStatusChild={syncStatusChild}
          />
        )}
      </Suspense>
    </LayoutMain>
  );
}
