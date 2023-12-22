import prisma from '@/app/utils/prisma';

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  const { flowName } = await request.json();
  const errs = await prisma.flow_errors.findMany({
    where: {
      flow_name: flowName,
    },
  });

  return new Response(
    JSON.stringify(errs, (_, v) => (typeof v === 'bigint' ? v.toString() : v))
  );
}
