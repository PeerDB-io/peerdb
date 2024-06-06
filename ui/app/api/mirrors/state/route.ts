import {
  MirrorStatusRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body: MirrorStatusRequest = await request.json();
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const res: MirrorStatusResponse = await flowServiceClient
      .get<MirrorStatusResponse>(`/v1/mirrors/${body.flowJobName}?`, {
        params: {
          include_flow_info: 'true',
        },
        headers: {
          cache: 'no-store',
        },
      })
      .then((res) => res.data);

    return new Response(JSON.stringify(res));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.error(message, e);
  }
}
