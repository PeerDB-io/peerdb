import { GetPeerDBClickhouseMode } from '@/peerdb-env/allowed_targets';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const allWarehouseTypes = [
    'Warehouses',
    'SNOWFLAKE',
    'BIGQUERY',
    'S3',
    'CLICKHOUSE',
    'ELASTICSEARCH',
  ];
  const clickhouseWarehouseTypes = ['Targets', 'CLICKHOUSE'];
  const queueTypes = [
    'Queues',
    'REDPANDA',
    'CONFLUENT',
    'KAFKA',
    'EVENTHUBS',
    'PUBSUB',
  ];
  const postgresTypes = [
    'Sources',
    'POSTGRESQL',
    'RDS POSTGRESQL',
    'GOOGLE CLOUD POSTGRESQL',
    'AZURE FLEXIBLE POSTGRESQL',
    'TEMBO',
    'CRUNCHY POSTGRES',
    'NEON',
  ];

  if (GetPeerDBClickhouseMode()) {
    return new Response(
      JSON.stringify([postgresTypes, clickhouseWarehouseTypes])
    );
  }
  return new Response(
    JSON.stringify([postgresTypes, allWarehouseTypes, queueTypes])
  );
}
