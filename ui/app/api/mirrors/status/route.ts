import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

function getMirrorStatusUrl(mirrorId: string) {
  let base = GetFlowHttpAddressFromEnv();
  return `${base}/v1/mirrors/${mirrorId}`;
}

export async function POST(request: Request) {
  const { flowJobName } = await request.json();
  const url = getMirrorStatusUrl(flowJobName);
  console.log(`querying ${url} for flow status.`)
  const resp = await fetch(url);
  const json = await resp.json();
  return new Response(JSON.stringify(json));
}
