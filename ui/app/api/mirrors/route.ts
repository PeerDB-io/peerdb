import { MirrorsListing } from '@/app/dto/MirrorsDTO';
import prisma from '@/app/utils/prisma';

export const dynamic = 'force-dynamic';

export async function GET(_request: Request) {
  const mirrors = await prisma.flows.findMany({
    distinct: 'name',
    include: {
      sourcePeer: true,
      destinationPeer: true,
    },
  });

  const flows: MirrorsListing[] = mirrors?.map((mirror) => ({
    id: mirror.id,
    workflowId: mirror.workflow_id,
    name: mirror.name,
    sourceName: mirror.sourcePeer.name,
    sourceType: mirror.sourcePeer.type,
    destinationName: mirror.destinationPeer.name,
    destinationType: mirror.destinationPeer.type,
    createdAt: mirror.created_at,
    isCdc: !mirror.query_string,
  }));
  return new Response(JSON.stringify(flows));
}
