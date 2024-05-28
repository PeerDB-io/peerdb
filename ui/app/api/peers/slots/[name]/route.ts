import { SlotLagPoint } from '@/app/dto/PeersDTO';
import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { name: string } }
) {
  const timeSince = request.nextUrl.searchParams.get('timeSince');

  let forThePastThisMuchTime: number;
  switch (timeSince) {
    case 'day':
      forThePastThisMuchTime = 86400000;
      break;
    case 'month':
      forThePastThisMuchTime = 2592000000;
      break;
    case '15min':
      forThePastThisMuchTime = 900000;
      break;
    case '5min':
      forThePastThisMuchTime = 300000;
      break;
    case '1min':
      forThePastThisMuchTime = 60000;
      break;
    default:
      forThePastThisMuchTime = 3600000;
      break;
  }

  const lagPoints = await prisma.$queryRaw<
    { updated_at: Date; slot_size: bigint }[]
  >`
    select updated_at, slot_size
    from peerdb_stats.peer_slot_size
    where slot_size is not null
      and slot_name = ${context.params.name}
      and updated_at > ${new Date(Date.now() - forThePastThisMuchTime)}
    order by random()
    limit 720
  `;

  const slotLagPoints: SlotLagPoint[] = lagPoints.map((lagPoint) => ({
    updatedAt: +lagPoint.updated_at,
    slotSize: Number(lagPoint.slot_size) / 1000,
  }));

  return NextResponse.json(slotLagPoints);
}
