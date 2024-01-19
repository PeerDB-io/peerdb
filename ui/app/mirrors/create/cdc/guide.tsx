import { DBTypeToGoodText } from '@/components/PeerTypeComponent';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import Link from 'next/link';

const GuideForDestinationSetup = ({ dstPeerType }: { dstPeerType: DBType }) => {
  const linkForDst = () => {
    switch (dstPeerType) {
      case DBType.SNOWFLAKE:
        return 'https://docs.peerdb.io/connect/snowflake';
      case DBType.BIGQUERY:
        return 'https://docs.peerdb.io/connect/bigquery';
      default:
        return 'https://docs.peerdb.io/';
    }
  };
  if (dstPeerType != DBType.SNOWFLAKE && dstPeerType != DBType.BIGQUERY) {
    return <></>;
  }
  return (
    <Label variant='body' as='label' style={{ marginBottom: '1rem' }}>
      We recommend going through our{' '}
      <Link style={{ color: 'teal' }} href={linkForDst()} target='_blank'>
        setup guide for {DBTypeToGoodText(dstPeerType)} destinations
      </Link>
      .
    </Label>
  );
};

export default GuideForDestinationSetup;
