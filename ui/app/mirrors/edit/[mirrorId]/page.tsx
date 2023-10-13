'use client';

import { MirrorStatusResponse } from '@/grpc_generated/route';
import { LayoutMain } from '@/lib/Layout';
import { ProgressCircle } from '@/lib/ProgressCircle';
import useSWR from 'swr';
import { CDCMirror } from './cdc';

type EditMirrorProps = {
  params: { mirrorId: string };
};

async function fetcher([url, mirrorId]: [string, string]) {
  return fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      flowJobName: mirrorId,
    }),
  })
    .then((res) => {
      if (!res.ok) throw new Error('Error fetching mirror status');
      return res.json();
    })
    .then((res: MirrorStatusResponse) => res);
}

export default function EditMirror({ params: { mirrorId } }: EditMirrorProps) {
  const {
    data: mirrorStatus,
    error,
    isValidating,
  } = useSWR(() => [`/api/mirrors/status`, mirrorId], fetcher);

  if (isValidating) {
    return <ProgressCircle variant='intermediate_progress_circle' />;
  }

  if (error) {
    console.error('Error fetching mirror status:', error);
    return <div>Error occurred!</div>;
  }

  if (!mirrorStatus) {
    return <div>No mirror status found!</div>;
  }

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <h1>{mirrorStatus.flowJobName}</h1>
      {mirrorStatus.cdcStatus && <CDCMirror cdc={mirrorStatus.cdcStatus} />}
    </LayoutMain>
  );
}
