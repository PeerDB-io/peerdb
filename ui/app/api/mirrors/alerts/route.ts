import prisma from '@/app/utils/prisma';

export async function POST(request: Request) {
  const { flowName } = await request.json();
  const errCount = await prisma.flow_errors.count({
    where: {
      flow_name: flowName,
      error_type: 'error',
      ack: false,
    },
  });
  let mirrorStatus: 'healthy' | 'failed';
  if (errCount > 0) {
    mirrorStatus = 'failed';
  } else {
    mirrorStatus = 'healthy';
  }
  return new Response(JSON.stringify(mirrorStatus));
}

// We accept a list here in preparation for a Select All feature in UI
export async function PUT(request: Request) {
  const { mirrorIDStringList } = await request.json();
  const mirrorIDList: bigint[] = mirrorIDStringList.map((id: string) =>
    BigInt(id)
  );
  const success = await prisma.flow_errors.updateMany({
    where: {
      id: {
        in: mirrorIDList,
      },
    },
    data: {
      ack: true,
    },
  });
  return new Response(JSON.stringify(success.count));
}
