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

  const lagPoints = await prisma.peer_slot_size.findMany({
    select: {
      updated_at: true,
      slot_size: true,
      wal_status: true,
    },
    where: {
      slot_name: context.params.name,
      updated_at: {
        gte: new Date(Date.now() - forThePastThisMuchTime),
      },
    },
    take: 100,
  });

  // convert slot_size to string
  const stringedLagPoints: SlotLagPoint[] = lagPoints.map((lagPoint) => {
    return {
      // human readable
      updatedAt:
        lagPoint.updated_at.toDateString() +
        ' ' +
        lagPoint.updated_at.toLocaleTimeString(),
      slotSize: lagPoint.slot_size?.toString(),
      walStatus: lagPoint.wal_status,
    };
  });

  return NextResponse.json(stringedLagPoints);
}
