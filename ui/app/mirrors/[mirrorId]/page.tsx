import { MirrorStatusResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import ViewMirror from './ViewMirror';

async function getMirrorState(
  flowJobName: string
): Promise<MirrorStatusResponse> {
  const addr = GetFlowHttpAddressFromEnv();
  const res = await fetch(`${addr}/v1/mirrors/status`, {
    method: 'POST',
    cache: 'no-store',
    body: JSON.stringify({
      flow_job_name: flowJobName,
      include_flow_info: true,
      exclude_batches: true,
    }),
  });
  if (!res.ok) throw new Error('Mirror not found');
  return res.json();
}

type Props = {
  params: Promise<{ mirrorId: string }>;
};

export default async function ViewMirrorPage({ params }: Props) {
  const { mirrorId } = await params;
  const mirrorStatePromise = getMirrorState(mirrorId);
  return (
    <ViewMirror mirrorId={mirrorId} mirrorStatePromise={mirrorStatePromise} />
  );
}
