import { UDropMirrorResponse } from '@/app/dto/MirrorsDTO';
import prisma from '@/app/utils/prisma';
import { ShutdownRequest, ShutdownResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { getTruePeer } from '../../peers/getTruePeer';

const getPeerFromName = async (name: string) => {
  const catalogPeer = await prisma.peers.findUnique({
    where: {
      name,
    },
  });
  const peer = getTruePeer(catalogPeer!);
  return peer;
};
export async function POST(request: Request) {
  const body = await request.json();
  const { workflowId, flowJobName, sourcePeerName, destinationPeerName } = body;
  const sourcePeer = await getPeerFromName(sourcePeerName);
  const destinationPeer = await getPeerFromName(destinationPeerName);
  const flowServiceAddr = GetFlowHttpAddressFromEnv();
  const req: ShutdownRequest = {
    workflowId,
    flowJobName,
    sourcePeer,
    destinationPeer,
    removeFlowEntry: true,
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
