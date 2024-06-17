import { SlotLagPoint } from '@/app/dto/PeersDTO';
import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { name: string } }
) {
  const timeSince = request.nextUrl.searchParams.get('timeSince');


  const lagPoints: { updated_at: Date; slot_size: bigint }[] = await prisma.$queryRaw
    `
    select updated_at, slot_size
    from peerdb_stats.peer_slot_size
    where slot_size is not null
      and slot_name = ${context.params.name}
      and updated_at > (now()-${timeSince}::INTERVAL)
    order by random()
    limit 720
  `;

  const slotLagPoints: SlotLagPoint[] = lagPoints.map((lagPoint) => ({
    updatedAt: +lagPoint.updated_at,
    slotSize: Number(lagPoint.slot_size) / 1000,
  }));

  return NextResponse.json(slotLagPoints);
}
