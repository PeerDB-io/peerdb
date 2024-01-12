import {
  MirrorLog,
  MirrorLogsRequest,
  MirrorLogsResponse,
} from '@/app/dto/AlertDTO';
import prisma from '@/app/utils/prisma';

export async function POST(request: Request) {
  const body = await request.json();
  const alertReq: MirrorLogsRequest = body;
  const skip = (alertReq.page - 1) * alertReq.numPerPage;

  const mirrorErrors: MirrorLog[] = await prisma.flow_errors.findMany({
    where: {
      OR: [
        {
          flow_name: {
            contains: alertReq.flowJobName,
          },
        },
        {
          flow_name: alertReq.flowJobName,
        },
      ],
    },
    orderBy: {
      error_timestamp: 'desc',
    },
    select: {
      id: false,
      flow_name: true,
      error_message: true,
      error_type: true,
      error_timestamp: true,
      ack: true,
    },
    take: alertReq.numPerPage,
    skip,
  });

  const total = await prisma.flow_errors.count({
    where: {
      flow_name: alertReq.flowJobName,
    },
  });

  const alertRes: MirrorLogsResponse = {
    errors: mirrorErrors,
    total,
  };

  return new Response(JSON.stringify(alertRes), {
    headers: {
      'Content-Type': 'application/json',
    },
  });
}
