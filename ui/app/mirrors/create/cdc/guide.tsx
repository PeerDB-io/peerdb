import { Label } from '@/lib/Label';
import Link from 'next/link';

const GuideForDestinationSetup = ({
  dstPeerType: peerType,
}: {
  dstPeerType: string;
}) => {
  const linkForDst = () => {
    switch (peerType) {
      case 'SNOWFLAKE':
        return 'https://docs.peerdb.io/connect/snowflake';
      case 'BIGQUERY':
        return 'https://docs.peerdb.io/connect/bigquery';
      default:
        return 'https://docs.peerdb.io/';
    }
  };
  if (peerType != 'SNOWFLAKE' && peerType != 'BIGQUERY') {
    return <></>;
  }
  return (
    <Label variant='body' as='label' style={{ marginBottom: '1rem' }}>
      We recommend going through our{' '}
      <Link style={{ color: 'teal' }} href={linkForDst()} target='_blank'>
        setup guide for {peerType.toLowerCase()} destinations
      </Link>
      .
    </Label>
  );
};

export default GuideForDestinationSetup;
