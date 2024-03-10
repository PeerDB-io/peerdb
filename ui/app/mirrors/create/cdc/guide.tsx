import TitleCase from '@/app/utils/titlecase';
import { Label } from '@/lib/Label';
import Link from 'next/link';

const GuideForDestinationSetup = ({
  createPeerType: peerType,
}: {
  createPeerType: string;
}) => {
  const linkForDst = () => {
    console.log(peerType);
    switch (peerType.toUpperCase().replace(/%20/g, ' ')) {
      case 'SNOWFLAKE':
        return 'https://docs.peerdb.io/connect/snowflake';
      case 'BIGQUERY':
        return 'https://docs.peerdb.io/connect/bigquery';
      case 'RDS POSTGRESQL':
      case 'POSTGRESQL':
        return 'https://docs.peerdb.io/connect/rds_postgres';
      case 'AZURE FLEXIBLE POSTGRESQL':
        return 'https://docs.peerdb.io/connect/azure_flexible_server_postgres';
      case 'GOOGLE CLOUD POSTGRESQL':
        return 'https://docs.peerdb.io/connect/cloudsql_postgres';
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
        setup guide for {TitleCase(peerType.toUpperCase().replace(/%20/g, ' '))}{' '}
        destinations
      </Link>
      .
    </Label>
  );
};

export default GuideForDestinationSetup;
