import prisma from '@/app/utils/prisma';
export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const mirrorNames = await prisma.flow_errors.findMany({
    select: {
      flow_name: true,
    },
    // where flow_name is not like 'clone_%'
    where: {
      NOT: {
        flow_name: {
          startsWith: 'clone_',
        },
      },
    },
    distinct: ['flow_name'],
  });
  return new Response(
    JSON.stringify(mirrorNames.map((mirror) => mirror.flow_name))
  );
}
