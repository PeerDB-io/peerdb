import { GetPeerDBClickHouseMode } from '@/peerdb-env/allowed_targets';
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
  const postgresTypes: [
    string,
    ...Array<string | { label: string; url: string }>,
  ] = [
    'Sources',
    'POSTGRESQL',
    'RDS POSTGRESQL',
    'GOOGLE CLOUD POSTGRESQL',
    'AZURE FLEXIBLE POSTGRESQL',
    'TEMBO',
    'CRUNCHY POSTGRES',
    'NEON',
  ];
  if (process.env.SUPABASE_ID) {
    postgresTypes.push({
      label: 'SUPABASE',
      url: `https://api.supabase.com/v1/oauth/authorize?client_id=${encodeURIComponent(
        process.env.SUPABASE_ID
      )}&response_type=code&redirect_uri=${encodeURIComponent(
        process.env.SUPABASE_REDIRECT ?? ''
      )}&state=${encodeURIComponent(process.env.SUPABASE_OAUTH_STATE ?? '')}`,
    });
  }

  if (GetPeerDBClickHouseMode()) {
    return new Response(
      JSON.stringify([postgresTypes, clickhouseWarehouseTypes])
    );
  }
  return new Response(
    JSON.stringify([postgresTypes, allWarehouseTypes, queueTypes])
  );
}
