'use client';
import { notifyErr } from '@/app/utils/notify';
import { FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { useRouter } from 'next/navigation';
import { useTransition } from 'react';

export default function QrepStatusButtons({ mirrorId }: { mirrorId: string }) {
  const [pending, start] = useTransition();
  const { refresh } = useRouter();

  const setFlowState = (requestedFlowState: FlowStatus) =>
    start(async () => {
      const res = await fetch('/api/v1/mirrors/state_change', {
        method: 'POST',
        cache: 'no-store',
        body: JSON.stringify({
          flowJobName: mirrorId,
          requestedFlowState,
        }),
      });
      if (!res.ok) {
        notifyErr((await res.json()).message || res.statusText);
        return;
      }
      refresh();
    });

  return (
    <div style={{ display: 'flex', columnGap: '1rem' }}>
      <Button
        disabled={pending}
        onClick={() => setFlowState(FlowStatus.STATUS_PAUSED)}
      >
        Pause
      </Button>
      <Button
        disabled={pending}
        onClick={() => setFlowState(FlowStatus.STATUS_RUNNING)}
      >
        Resume
      </Button>
    </div>
  );
}
