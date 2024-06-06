import { FlowStateChangeResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const res: FlowStateChangeResponse =
      await flowServiceClient.post(
        `/v1/mirrors/state_change`,
        body
      );
    return new Response(JSON.stringify(res));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.error(message, e);
  }
}
