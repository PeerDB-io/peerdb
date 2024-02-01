import {
  MirrorStatusRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body: MirrorStatusRequest = await request.json();
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  console.log('/mirrors/state: req:', body);
  try {
    const res: MirrorStatusResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/${body.flowJobName}`,
      { cache: 'no-store' }
    ).then((res) => {
      return res.json();
    });

    return new Response(JSON.stringify(res));
  } catch (e) {
    console.error(e);
  }
}
