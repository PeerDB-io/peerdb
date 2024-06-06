import { UDropMirrorResponse } from '@/app/dto/MirrorsDTO';
import { ShutdownRequest, ShutdownResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { workflowId, flowJobName, sourcePeer, destinationPeer } = body;
  const flowServiceClient = GetFlowServiceHttpClient();
  const req: ShutdownRequest = {
    workflowId,
    flowJobName,
    sourcePeer,
    destinationPeer,
    removeFlowEntry: true,
  };

  try {
    const dropStatus: ShutdownResponse =
      await flowServiceClient.post(`/v1/mirrors/drop`, req);
    let response: UDropMirrorResponse = {
      dropped: dropStatus.ok,
      errorMessage: dropStatus.errorMessage,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    const message = await ParseFlowServiceErrorMessage(e);
    console.error(message, e);
  }
}
