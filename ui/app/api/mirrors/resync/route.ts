import {
  ResyncMirrorRequest,
  ResyncMirrorResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { flowJobName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: ResyncMirrorRequest = {
    flowJobName,
  };

  try {
    const res: ResyncMirrorResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/resync`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });

    return new Response(JSON.stringify(res));
  } catch (e) {
    if (e instanceof Response) {
      return e;
    }
    console.log(e);
  }
}
