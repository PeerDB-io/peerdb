import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  CreateCDCFlowRequest,
  CreateCDCFlowResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { config } = body;
  console.log('/mirrors/cdc config: ', config);
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: CreateCDCFlowRequest = {
    connectionConfigs: config,
    createCatalogEntry: true,
  };
  try {
    const createStatus: CreateCDCFlowResponse = await fetch(
      `${flowServiceAddr}/v1/flows/cdc/create`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });

    if (!createStatus.workflowId) {
      return new Response(JSON.stringify(createStatus));
    }
    let response: UCreateMirrorResponse = {
      created: true,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
