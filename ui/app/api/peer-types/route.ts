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
    {
      label: 'SUPABASE',
      url: `https://api.supabase.com/v1/oauth/authorize?client_id=${encodeURIComponent(
        process.env.SUPABASE_ID ?? ''
      )}&response_type=code&redirect_uri=${encodeURIComponent(
        process.env.SUPABASE_REDIRECT ?? ''
      )}`,
    },
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
