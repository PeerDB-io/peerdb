import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export const dynamic = 'force-dynamic';

// this should actually cache since it is a constant
export async function GET() {
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  await fetch(`${flowServiceAddr}/v1/version`);
}
