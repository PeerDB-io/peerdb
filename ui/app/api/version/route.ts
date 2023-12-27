import { UPeerDBVersion } from '@/app/dto/VersionDTO';
import { PeerDBVersionResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const versionResponse: PeerDBVersionResponse = await fetch(
      `${flowServiceAddr}/v1/version`,
      {
        method: 'GET',
      }
    ).then((res) => {
      return res.json();
    });
    let response: UPeerDBVersion = {
      version: versionResponse.version,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
