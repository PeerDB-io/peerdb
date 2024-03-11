import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  CreateQRepFlowRequest,
  CreateQRepFlowResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: CreateQRepFlowRequest = body;
  try {
    const createStatus: CreateQRepFlowResponse = await fetch(
      `${flowServiceAddr}/v1/flows/qrep/create`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });
    let response: UCreateMirrorResponse = {
      created: !!createStatus.workflowId,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    return new Response(JSON.stringify({ created: false }));
    console.log(e);
  }
}
