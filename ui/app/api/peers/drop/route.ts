import { UDropPeerResponse } from '@/app/dto/PeersDTO';
import { DropPeerRequest, DropPeerResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: DropPeerRequest = {
    peerName,
  };
  console.log('/drop/peer: req:', req);
  const dropStatus: DropPeerResponse = await fetch(
    `${flowServiceAddr}/v1/peers/drop`,
    {
      method: 'POST',
      body: JSON.stringify(req),
    }
  ).then((res) => {
    return res.json();
  });
  let response: UDropPeerResponse = {
    dropped: dropStatus.ok,
    errorMessage: dropStatus.errorMessage,
  };

  return new Response(JSON.stringify(response));
}
