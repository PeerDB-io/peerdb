import { UDropMirrorResponse } from '@/app/dto/MirrorsDTO';
import { ShutdownRequest, ShutdownResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { flowJobName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: ShutdownRequest = {
    flowJobName,
  };

  try {
    const dropStatus: ShutdownResponse = await fetch(
      `${flowServiceAddr}/v1/mirrors/drop`,
      {
        method: 'POST',
        body: JSON.stringify(req),
      }
    ).then((res) => {
      return res.json();
    });
    let response: UDropMirrorResponse = {
      dropped: dropStatus.ok,
      errorMessage: dropStatus.errorMessage,
    };

    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
