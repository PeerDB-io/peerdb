import { MirrorStatusRequest } from '@/grpc_generated/route';
import { GetFlowServiceHttpClient } from '@/rpc/http';

export async function POST(request: Request) {
  const body: MirrorStatusRequest = await request.json();
  const flowServiceClient = GetFlowServiceHttpClient();
  return flowServiceClient.raw(
    `/v1/mirrors/${body.flowJobName}?include_flow_info=true`,
    { cache: 'no-store' }
  );
}
