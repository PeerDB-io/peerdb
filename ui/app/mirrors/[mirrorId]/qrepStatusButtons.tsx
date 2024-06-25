'use client';
import { FlowStatus } from '@/grpc_generated/flow';

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
    <>
      <input
        type='button'
        value='Pause'
        onClick={() => setFlowState(props.mirrorId, FlowStatus.STATUS_PAUSED)}
      />
      <input
        type='button'
        value='Resume'
        onClick={() => setFlowState(props.mirrorId, FlowStatus.STATUS_RUNNING)}
      />
    </>
  );
}
