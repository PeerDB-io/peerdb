import { FlowConnectionConfigs, FlowStatus } from '@/grpc_generated/flow';
import {
  FlowStateChangeRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';

export const getMirrorState = async (mirrorId: string) => {
  return await fetch('/api/mirrors/state', {
    method: 'POST',
    body: JSON.stringify({
      flowJobName: mirrorId,
    }),
  }).then((res) => res.json());
};

export const getCurrentIdleTimeout = async (mirrorId: string) => {
  const res = await getMirrorState(mirrorId);
  return (res as MirrorStatusResponse).cdcStatus?.config?.idleTimeoutSeconds;
};

export const changeFlowState = async (
  mirrorConfig: FlowConnectionConfigs,
  flowState: FlowStatus
) => {
  const req: FlowStateChangeRequest = {
    flowJobName: mirrorConfig.flowJobName,
    sourcePeer: mirrorConfig.sourceName,
    destinationPeer: mirrorConfig.destinationName,
    requestedFlowState: flowState,
  };
  await fetch(`/api/mirrors/state_change`, {
    method: 'POST',
    body: JSON.stringify(req),
    cache: 'no-store',
  });
  window.location.reload();
};
