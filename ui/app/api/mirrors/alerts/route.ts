import prisma from '@/app/utils/prisma';

export const dynamic = 'force-dynamic';

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
