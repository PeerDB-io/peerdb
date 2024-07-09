import { ScriptsType } from '@/app/dto/ScriptsDTO';
import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  _: NextRequest,
  context: { params: { scriptId: string } }
) {
  const existingScript = await prisma.scripts.findFirst({
    where: {
      id: parseInt(context.params.scriptId, 10),
    },
  });

  const script: ScriptsType = {
    ...existingScript!,
    source: existingScript!.source.toString(),
  };
  return NextResponse.json(script);
}
