import { MirrorStatusResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();

  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const res: MirrorStatusResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/${body.mirrorName}?` +
        new URLSearchParams({
          include_flow_info: 'true',
        }),
      {
        cache: 'no-store',
      }
    ).then((res) => {
      return res.json();
    });

    return new Response(JSON.stringify(res));
  } catch (e) {
    console.error(e);
  }
}
