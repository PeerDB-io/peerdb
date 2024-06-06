import {
  CreateCDCFlowRequest,
  ValidateCDCMirrorResponse,
} from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const { config } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  const req: CreateCDCFlowRequest = {
    connectionConfigs: config,
  };
  try {
    const validateResponse: ValidateCDCMirrorResponse =
      await flowServiceClient.post(`/v1/mirrors/cdc/validate`, req);
    return new Response(JSON.stringify(validateResponse));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
