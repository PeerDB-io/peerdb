import { FlowStateChangeResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const flowServiceAddr = GetFlowHttpAddressFromEnv();

  try {
    const res: FlowStateChangeResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/state_change`,
      {
        method: 'POST',
        body: JSON.stringify(body),
      }
    ).then((res) => {
      return res.json();
    });

    return new Response(JSON.stringify(res));
  } catch (e) {
    console.error(e);
  }
}
