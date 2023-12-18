import { getTruePeer } from '@/app/api/peers/getTruePeer';
import prisma from '@/app/utils/prisma';

export const dynamic = 'force-dynamic';

const stringifyConfig = (flowArray: any[]) => {
  flowArray.forEach((flow) => {
    if (flow.config_proto) {
      flow.config_proto = new TextDecoder().decode(flow.config_proto);
    }
  });
  return flowArray;
};

export async function GET(request: Request) {
  const mirrors = await prisma.flows.findMany({
    distinct: 'name',
    include: {
      sourcePeer: true,
      destinationPeer: true,
    },
  });

  const flows = mirrors?.map((mirror) => {
    let newMirror: any = {
      ...mirror,
      sourcePeer: getTruePeer(mirror.sourcePeer),
      destinationPeer: getTruePeer(mirror.destinationPeer),
    };
    return newMirror;
  });
  return new Response(JSON.stringify(stringifyConfig(flows)));
}
