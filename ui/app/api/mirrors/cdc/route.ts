import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import { CreateCDCFlowRequest } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { config } = body;

  const flowServiceClient = GetFlowServiceHttpClient();
  const req: CreateCDCFlowRequest = {
    connectionConfigs: config,
  };
  try {
    const createStatus = await flowServiceClient.post(
      `/v1/flows/cdc/create`,
      req
    );
    if (!createStatus.workflowId) {
      return new Response(JSON.stringify(createStatus));
    }
    let response: UCreateMirrorResponse = {
      created: true,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.log(message, e);
  }
}
