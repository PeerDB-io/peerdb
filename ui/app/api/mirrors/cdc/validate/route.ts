import {
  CreateCDCFlowRequest,
  ValidateCDCMirrorResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { NextRequest } from 'next/server';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const { config } = body;
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
