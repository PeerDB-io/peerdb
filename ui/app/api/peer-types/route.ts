import { GetPeerDBClickHouseMode } from '@/peerdb-env/allowed_targets';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';

type PeerTypeItem = {
  label: string;
  url?: string;
  deprecated?: boolean;
  // When deprecation only applies to the connector's use as a destination
  // (e.g. BigQuery is still a supported source), the badge reflects that.
  deprecatedRole?: 'destination';
};
type PeerTypeCategory = [string, ...Array<string | PeerTypeItem>];

export async function GET(request: NextRequest) {
  const allWarehouseTypes: PeerTypeCategory = [
    'Warehouses',
    { label: 'SNOWFLAKE', deprecated: true },
    // BigQuery remains a supported source; only its destination role is deprecated.
    { label: 'BIGQUERY', deprecated: true, deprecatedRole: 'destination' },
    { label: 'S3', deprecated: true },
    'CLICKHOUSE',
    { label: 'ELASTICSEARCH', deprecated: true },
  ];
  const clickhouseWarehouseTypes: PeerTypeCategory = ['Targets', 'CLICKHOUSE'];
  const queueTypes: PeerTypeCategory = [
    'Queues',
    { label: 'REDPANDA', deprecated: true },
    { label: 'CONFLUENT', deprecated: true },
    { label: 'KAFKA', deprecated: true },
    { label: 'EVENTHUBS', deprecated: true },
    { label: 'PUBSUB', deprecated: true },
  ];
  const postgresTypes: PeerTypeCategory = [
    'Sources',
    'MYSQL',
    'POSTGRESQL',
    'RDS POSTGRESQL',
    'GOOGLE CLOUD POSTGRESQL',
    'AZURE FLEXIBLE POSTGRESQL',
    'CRUNCHY POSTGRES',
    'NEON',
    'MONGO',
    'COCKROACHDB',
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
