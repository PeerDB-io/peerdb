import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  CreateQRepFlowRequest,
  CreateQRepFlowResponse,
} from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { config } = body;

  const flowServiceClient = GetFlowServiceHttpClient();
  const req: CreateQRepFlowRequest = {
    qrepConfig: config,
    createCatalogEntry: true,
  };
  try {
    const createStatus: CreateQRepFlowResponse = await flowServiceClient
      .post<CreateQRepFlowResponse>(`/v1/flows/qrep/create`, req)
      .then((res) => res.data);
    let response: UCreateMirrorResponse = {
      created: !!createStatus.workflowId,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = ParseFlowServiceErrorMessage(e);
    console.error(message, e);
  }
}
