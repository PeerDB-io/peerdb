'use client';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { useRouter } from 'next/navigation';

export const DBTypeToImageMapping = (peerType: DBType) => {
  switch (peerType) {
    case DBType.POSTGRES:
      return 'svgs/pg.svg';
    case DBType.SNOWFLAKE:
      return 'svgs/sf.svg';
    case DBType.BIGQUERY:
      return 'svgs/bq.svg';
    case DBType.EVENTHUB_GROUP:
      return 'svgs/ms.svg';
    case DBType.EVENTHUB:
      return 'svgs/ms.svg';
    default:
      return '';
  }
};

const PeerButton = ({
  peerName,
  peerType,
}: {
  peerName: string;
  peerType: DBType;
}) => {
  const router = useRouter();
  return (
    <Button
      style={{
        backgroundColor: 'white',
        color: 'white',
        fontSize: 13,
        padding: '0.5rem',
        boxShadow: '2px 1px 2px 2px rgba(0,0,0,0.1)',
        borderRadius: '0.5rem',
      }}
      onClick={() => router.push(`/peers/${peerName}`)}
    >
      <img
        src={DBTypeToImageMapping(peerType)}
        style={{ height: '1.5rem' }}
      ></img>
      <Label>{peerName}</Label>
    </Button>
  );
};

export default PeerButton;
