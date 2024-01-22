import {
  CreateCDCFlowRequest,
  ValidateCDCMirrorResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { config } = body;
  console.log('/mirrors/cdc/validate config: ', config);
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: CreateCDCFlowRequest = {
    connectionConfigs: config,
    createCatalogEntry: false,
  };
  try {
    const validateResponse: ValidateCDCMirrorResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/cdc/validate`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });

    return new Response(JSON.stringify(validateResponse));
  } catch (e) {
    console.log(e);
  }
}
