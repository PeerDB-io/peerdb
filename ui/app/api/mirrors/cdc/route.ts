import {
  CreateCDCFlowRequest,
  CreateCDCFlowResponse,
} from '@/grpc_generated/route';
import { GetFlowServiceClientFromEnv } from '@/rpc/rpc';

export async function POST(request: Request) {
  const body = await request.json();
  const { config } = body;
  const flowServiceClient = GetFlowServiceClientFromEnv();
  const req: CreateCDCFlowRequest = {
    connectionConfigs: config,
  };
  const createStatus: CreateCDCFlowResponse =
    await flowServiceClient.createCdcFlow(req);
  if (!createStatus.worflowId) {
    return new Response('Failed to create CDC mirror');
  }
  return new Response('created');
}
