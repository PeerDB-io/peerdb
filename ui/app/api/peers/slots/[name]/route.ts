import { SlotLagPoint } from '@/app/dto/PeersDTO';
import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { name: string } }
) {
  const lagPoints = await prisma.peer_slot_size.findMany({
    select: {
      updated_at: true,
      slot_size: true,
      wal_status: true,
    },
    where: {
      slot_name: context.params.name,
    },
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
