import { PeerDBVersionResponse } from '@/grpc_generated/route';
import {
  GetFlowServiceHttpClient,
  ParseFlowServiceErrorMessage,
} from '@/rpc/http';

export const dynamic = 'force-dynamic';

export async function GET() {
  const flowServiceClient = GetFlowServiceHttpClient();
  try {
    const versionResponse =
      await flowServiceClient.get(`/v1/version`);
    let response = {
      version: versionResponse.version,
    };
    return new Response(JSON.stringify(response));
  } catch (error) {
    const message = await ParseFlowServiceErrorMessage(error);
    console.error('Error getting version:', message);
    return new Response(JSON.stringify({ error: message }));
  }
}
