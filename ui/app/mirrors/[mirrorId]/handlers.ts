import { FlowStatus } from '@/grpc_generated/flow';
import {
  FlowStateChangeRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';

export const getMirrorState = async (
  mirrorName: string
): Promise<MirrorStatusResponse> => {
  const res = await fetch('/api/mirrors/state', {
    method: 'POST',
    body: JSON.stringify({
      mirrorName: mirrorName,
    }),
  });
  return await res.json();
};

export const getCurrentIdleTimeout = async (mirrorName: string) => {
  const res = await getMirrorState(mirrorName);
  return (res as MirrorStatusResponse).cdcStatus?.config?.idleTimeoutSeconds;
};

export const changeFlowState = async (
  mirrorName: string,
  flowState: FlowStatus
): Promise<Response> => {
  const req: FlowStateChangeRequest = {
    flowJobName: mirrorName,
    requestedFlowState: flowState,
  };
  const res = await fetch(`/api/mirrors/state_change`, {
    method: 'POST',
    body: JSON.stringify(req),
    cache: 'no-store',
  });
  window.location.reload();
  return res;
};
