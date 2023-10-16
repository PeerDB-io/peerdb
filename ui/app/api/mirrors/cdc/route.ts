import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  CreateCDCFlowRequest,
  CreateCDCFlowResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { config } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: CreateCDCFlowRequest = {
    connectionConfigs: config,
    createCatalogEntry: true,
  };
  const createStatus: CreateCDCFlowResponse = await fetch(
    `${flowServiceAddr}/v1/cdc/create`,
    {
      method: 'POST',
      body: JSON.stringify(req),
    }
  ).then((res) => {
    return res.json();
  });

  let response: UCreateMirrorResponse = {
    created: !!createStatus.worflowId,
  };

  return new Response(JSON.stringify(response));
}
