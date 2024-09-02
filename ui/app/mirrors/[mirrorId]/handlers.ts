import { FlowStatus } from '@/grpc_generated/flow';
import {
  FlowStateChangeRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';

export const getMirrorState = async (
  flow_job_name: string
): Promise<MirrorStatusResponse> => {
  const res = await fetch('/api/v1/mirrors/status', {
    method: 'POST',
    body: JSON.stringify({
      flow_job_name,
      include_flow_info: true,
    }),
  });
  return res.json();
};

export const getCurrentIdleTimeout = async (mirrorName: string) => {
  const res = await getMirrorState(mirrorName);
  return (res as MirrorStatusResponse).cdcStatus?.config?.idleTimeoutSeconds;
};

export const changeFlowState = async (
  mirrorName: string,
  flowState: FlowStatus,
  dropStats?: boolean
): Promise<Response> => {
  const req: FlowStateChangeRequest = {
    flowJobName: mirrorName,
    requestedFlowState: flowState,
    dropMirrorStats: dropStats ?? false,
  };
  const res = await fetch('/api/v1/mirrors/state_change', {
    method: 'POST',
    body: JSON.stringify(req),
    cache: 'no-store',
  });
  window.location.reload();
  return res;
};
