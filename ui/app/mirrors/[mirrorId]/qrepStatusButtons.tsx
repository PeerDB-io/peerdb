'use client';
import { FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';

async function setFlowState(
  flowJobName: string,
  requestedFlowState: FlowStatus
) {
  await fetch(`/api/mirrors/state_change`, {
    method: 'POST',
    body: JSON.stringify({
      flowJobName,
      requestedFlowState,
    }),
    cache: 'no-store',
  });
  window.location.reload();
}

export default function qrepStatusButtons(props: { mirrorId: string }) {
  return (
    <div style={{ display: 'flex', columnGap: '1rem' }}>
      <Button
        onClick={() => setFlowState(props.mirrorId, FlowStatus.STATUS_PAUSED)}
      >
        Pause
      </Button>
      <Button
        onClick={() => setFlowState(props.mirrorId, FlowStatus.STATUS_RUNNING)}
      >
        Resume
      </Button>
    </div>
  );
}
