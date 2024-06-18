import {
  CreateCustomSyncRequest,
  CreateCustomSyncResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { flowJobName, numberOfSyncs } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: CreateCustomSyncRequest = {
    flowJobName: flowJobName,
    numberOfSyncs: numberOfSyncs,
  };
  try {
    const customSyncResponse: CreateCustomSyncResponse = await fetch(
      `${flowServiceAddr}/v1/flows/cdc/sync`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });
    return new Response(JSON.stringify(customSyncResponse));
  } catch (e) {
    console.log(e);
  }
}
