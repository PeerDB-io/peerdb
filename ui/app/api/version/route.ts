import { UVersionResponse } from '@/app/dto/VersionDTO';
import { PeerDBVersionResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export const dynamic = 'force-dynamic';

export async function GET() {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const versionResponse: PeerDBVersionResponse = await fetch(
      `${flowServiceAddr}/v1/version`
    ).then((res) => res.json());
    let response: UVersionResponse = {
      version: versionResponse.version,
    };
    return new Response(JSON.stringify(response));
  } catch (error) {
    console.error('Error getting version:', error);
    return new Response(JSON.stringify({ error: error }));
  }
}
