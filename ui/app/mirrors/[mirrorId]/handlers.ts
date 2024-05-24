import { DefaultSyncInterval } from '@/app/utils/defaultMirrorSettings';
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
  return await getMirrorState(mirrorId).then((res) => {
    return (
      (res as MirrorStatusResponse).cdcStatus?.config?.idleTimeoutSeconds ||
      DefaultSyncInterval
    );
  });
};

export const changeFlowState = async (
  mirrorConfig: FlowConnectionConfigs,
  flowState: FlowStatus
) => {
  const req: FlowStateChangeRequest = {
    flowJobName: mirrorConfig.flowJobName,
    sourcePeer: mirrorConfig.source,
    destinationPeer: mirrorConfig.destination,
    requestedFlowState: flowState,
  };
  await fetch(`/api/mirrors/state_change`, {
    method: 'POST',
    body: JSON.stringify(req),
    cache: 'no-store',
  });
  window.location.reload();
};
