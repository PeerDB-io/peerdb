import { GetHashedPeerDBPasswordFromEnv } from '@/app/utils/passwordFromEnv';
import {
  MirrorStatusRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body: MirrorStatusRequest = await request.json();
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const authToken = GetHashedPeerDBPasswordFromEnv();
  try {
    const res: MirrorStatusResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/${body.flowJobName}?` +
        new URLSearchParams({
          include_flow_info: 'true',
        }),
      {
        cache: 'no-store',
        headers: new Headers({ Authorization: `Bearer ${authToken}` }),
      }
    ).then((res) => {
      return res.json();
    });

    return new Response(JSON.stringify(res));
  } catch (e) {
    console.error(e);
  }
}
