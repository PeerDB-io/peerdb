import TitleCase from '@/app/utils/titlecase';
import { Label } from '@/lib/Label';
import Link from 'next/link';

export default function GuideForDestinationSetup({
  createPeerType: peerType,
}: {
  createPeerType: string;
}) {
  const linkForDst = () => {
    switch (peerType) {
      case 'SNOWFLAKE':
        return 'https://docs.peerdb.io/connect/snowflake';
      case 'BIGQUERY':
        return 'https://docs.peerdb.io/connect/bigquery';
      case 'RDS POSTGRESQL':
        return 'https://docs.peerdb.io/connect/postgres/rds_postgres';
      case 'POSTGRESQL':
        return 'https://docs.peerdb.io/connect/postgres/generic_postgres';
      case 'AZURE FLEXIBLE POSTGRESQL':
        return 'https://docs.peerdb.io/connect/postgres/azure_flexible_server_postgres';
      case 'GOOGLE CLOUD POSTGRESQL':
        return 'https://docs.peerdb.io/connect/postgres/cloudsql_postgres';
      case 'CRUNCHY POSTGRES':
        return 'https://docs.peerdb.io/connect/postgres/crunchy_bridge';
      case 'NEON':
        return 'https://docs.peerdb.io/connect/postgres/neon_postgres';
      case 'CONFLUENT':
        return 'https://docs.peerdb.io/connect/confluent-cloud';
      case 'REDPANDA':
      case 'KAFKA':
        return 'https://docs.peerdb.io/connect/kafka';
      case 'CLICKHOUSE':
        return 'https://docs.peerdb.io/connect/clickhouse';
      default:
        return '';
    }
  };
  if (linkForDst() == '') {
    return <></>;
  }
  return (
    <Label variant='body' as='label' style={{ marginBottom: '1rem' }}>
      We recommend going through our{' '}
      <Link style={{ color: 'teal' }} href={linkForDst()} target='_blank'>
        setup guide for {TitleCase(peerType)} destinations
      </Link>
      .
    </Label>
  );
}
