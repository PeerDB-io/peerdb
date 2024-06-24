import prisma from '@/app/utils/prisma';
import { NextRequest, NextResponse } from 'next/server';

import { ElasticsearchAuthType } from '@/grpc_generated/peers';
import { getTruePeer } from '../../getTruePeer';

export async function GET(
  _request: NextRequest,
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
  const mgConfig = peerConfig.mongoConfig;
  const s3Config = peerConfig.s3Config;
  const sfConfig = peerConfig.snowflakeConfig;
  const ehConfig = peerConfig.eventhubGroupConfig;
  const chConfig = peerConfig.clickhouseConfig;
  const kaConfig = peerConfig.kafkaConfig;
  const psConfig = peerConfig.pubsubConfig;
  const esConfig = peerConfig.elasticsearchConfig;
  const myConfig = peerConfig.mysqlConfig;

  const redactString = '********';
  if (pgConfig) {
    pgConfig.password = redactString;

    if (pgConfig.sshConfig) {
      pgConfig.sshConfig.password = redactString;
      pgConfig.sshConfig.privateKey = redactString;
      pgConfig.sshConfig.hostKey = redactString;
    }
  } else if (bqConfig) {
    bqConfig.privateKey = redactString;
    bqConfig.privateKeyId = redactString;
  } else if (mgConfig) {
    mgConfig.password = redactString;
  } else if (s3Config) {
    s3Config.secretAccessKey = redactString;
  } else if (sfConfig) {
    sfConfig.privateKey = redactString;
    sfConfig.password = redactString;
  } else if (ehConfig) {
    for (const key in ehConfig.eventhubs) {
      ehConfig.eventhubs[key].subscriptionId = redactString;
    }
  } else if (chConfig) {
    chConfig.password = redactString;
    chConfig.accessKeyId = redactString;
    chConfig.secretAccessKey = redactString;
  } else if (kaConfig) {
    kaConfig.password = redactString;
  } else if (psConfig?.serviceAccount) {
    psConfig.serviceAccount.privateKey = redactString;
    psConfig.serviceAccount.privateKeyId = redactString;
  } else if (esConfig) {
    if (esConfig.authType === ElasticsearchAuthType.BASIC) {
      esConfig.username = redactString;
      esConfig.password = redactString;
    } else if (esConfig.authType === ElasticsearchAuthType.APIKEY) {
      esConfig.apiKey = redactString;
    }
  } else if (myConfig) {
    myConfig.password = redactString;
  }

  return NextResponse.json(peerConfig);
}
