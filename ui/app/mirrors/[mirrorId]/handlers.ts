import { FlowStatus } from '@/grpc_generated/flow';
import {
  FlowStateChangeRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';

export async function getMirrorState(
  flow_job_name: string
): Promise<MirrorStatusResponse> {
  const res = await fetch('/api/v1/mirrors/status', {
    method: 'POST',
    body: JSON.stringify({
      flow_job_name,
      include_flow_info: true,
      exclude_batches: true,
    }),
  });
  if (!res.ok) throw res.json();
  return res.json();
}

export async function getCurrentIdleTimeout(mirrorName: string) {
  const res = await getMirrorState(mirrorName);
  return (res as MirrorStatusResponse).cdcStatus?.config?.idleTimeoutSeconds;
}

export async function changeFlowState(
  mirrorName: string,
  flowState: FlowStatus,
  dropStats?: boolean
): Promise<Response> {
  const req: FlowStateChangeRequest = {
    flowJobName: mirrorName,
    requestedFlowState: flowState,
    dropMirrorStats: dropStats ?? false,
    skipDestinationDrop: false,
  };
  const res = await fetch('/api/v1/mirrors/state_change', {
    method: 'POST',
    body: JSON.stringify(req),
    cache: 'no-store',
  });
  window.location.reload();
  return res;
}
