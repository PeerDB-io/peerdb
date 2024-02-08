import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';
import { getTruePeer } from '../../getTruePeer';

export async function GET(
  request: NextRequest,
  context: { params: { peerName: string } }
) {
  const peerName = context.params.peerName;
  const peer = await prisma.peers.findFirst({
    where: {
      name: peerName,
    },
  });
  const peerConfig = getTruePeer(peer!);
  // omit sensitive keys
  const pgConfig = peerConfig.postgresConfig;
  const bqConfig = peerConfig.bigqueryConfig;
  const s3Config = peerConfig.s3Config;
  const sfConfig = peerConfig.snowflakeConfig;
  const ehConfig = peerConfig.eventhubConfig;
  if (pgConfig) {
    pgConfig.password = '********';
    pgConfig.transactionSnapshot = '********';
  }
  if (bqConfig) {
    bqConfig.privateKey = '********';
    bqConfig.privateKeyId = '********';
  }
  if (s3Config) {
    s3Config.secretAccessKey = '********';
  }
  if (sfConfig) {
    sfConfig.privateKey = '********';
    sfConfig.password = '********';
  }
  if (ehConfig) {
    ehConfig.subscriptionId = '********';
  }

  return NextResponse.json(peerConfig);
}
