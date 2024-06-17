import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

import { ElasticsearchAuthType } from '@/grpc_generated/peers';
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
  const ehConfig = peerConfig.eventhubGroupConfig;
  const chConfig = peerConfig.clickhouseConfig;
  const kaConfig = peerConfig.kafkaConfig;
  const psConfig = peerConfig.pubsubConfig;
  const esConfig = peerConfig.elasticsearchConfig;

  const redactString = '********';
  if (pgConfig) {
    pgConfig.password = redactString;
    pgConfig.transactionSnapshot = redactString;
  }
  if (bqConfig) {
    bqConfig.privateKey = redactString;
    bqConfig.privateKeyId = redactString;
  }
  if (s3Config) {
    s3Config.secretAccessKey = redactString;
  }
  if (sfConfig) {
    sfConfig.privateKey = redactString;
    sfConfig.password = redactString;
  }
  if (ehConfig) {
    for (const key in ehConfig.eventhubs) {
      ehConfig.eventhubs[key].subscriptionId = redactString;
    }
  }

  if (chConfig) {
    chConfig.password = redactString;
    chConfig.secretAccessKey = redactString;
  }
  if (kaConfig) {
    kaConfig.password = redactString;
  }
  if (psConfig?.serviceAccount) {
    psConfig.serviceAccount.privateKey = redactString;
    psConfig.serviceAccount.privateKeyId = redactString;
  }
  if (esConfig) {
    if (esConfig.authType === ElasticsearchAuthType.BASIC) {
      esConfig.username = redactString;
      esConfig.password = redactString;
    } else if (esConfig.authType === ElasticsearchAuthType.APIKEY) {
      esConfig.apiKey = redactString;
    }
  }

  return NextResponse.json(peerConfig);
}
