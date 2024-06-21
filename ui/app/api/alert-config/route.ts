import { alertConfigType } from '@/app/alert-config/validation';
import prisma from '@/app/utils/prisma';
import { alerting_config } from '@prisma/client';

export async function GET() {
  const configs: alerting_config[] = await prisma.alerting_config.findMany();
  const serializedConfigs = configs.map((config) => ({
    ...config,
    id: String(config.id),
  }));
  return new Response(JSON.stringify(serializedConfigs));
}

export async function POST(request: Request) {
  const alertConfigReq: alertConfigType = await request.json();
  const createRes = await prisma.alerting_config.create({
    data: {
      service_type: alertConfigReq.serviceType,
      service_config: alertConfigReq.serviceConfig,
    },
  });

  return new Response(createRes.id ? 'success' : 'error');
}

export async function DELETE(request: Request) {
  const configDeleteReq: { id: number } = await request.json();
  const deleteRes = await prisma.alerting_config.delete({
    where: {
      id: configDeleteReq.id,
    },
  });

  return new Response(deleteRes.id ? 'success' : 'error');
}

export async function PUT(request: Request) {
  const alertConfigReq: alertConfigType = await request.json();
  const editRes = await prisma.alerting_config.update({
    data: {
      service_type: alertConfigReq.serviceType,
      service_config: alertConfigReq.serviceConfig,
    },
    where: {
      id: alertConfigReq.id,
    },
  });

  return new Response(editRes.id ? 'success' : 'error');
}
