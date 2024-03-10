import { UPublicationsResponse } from '@/app/dto/PeersDTO';
import { PeerPublicationsResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';

export async function POST(request: Request) {
  const body = await request.json();
  const { peerName } = body;
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  try {
    const publicationList: PeerPublicationsResponse = await fetch(
      `${flowServiceAddr}/v1/peers/publications?peer_name=${peerName}`
    ).then((res) => {
      return res.json();
    });
    let response: UPublicationsResponse = {
      publicationNames: publicationList.publicationNames,
    };
    return new Response(JSON.stringify(response));
  } catch (e) {
    console.log(e);
  }
}
