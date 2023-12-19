import { getTruePeer } from '@/app/api/peers/getTruePeer';
import prisma from '@/app/utils/prisma';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const mirrors = await prisma.flows.findMany({
    distinct: 'name',
    include: {
      sourcePeer: true,
      destinationPeer: true,
    },
  });

  // using any as type because of the way prisma returns data
  const flows = mirrors?.map((mirror: any) => {
    let newMirror: any = {
      ...mirror,
      sourcePeer: getTruePeer(mirror.sourcePeer),
      destinationPeer: getTruePeer(mirror.destinationPeer),
    };
    return newMirror;
  });
  return new Response(JSON.stringify(flows));
}
