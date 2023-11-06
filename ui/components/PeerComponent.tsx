'use client';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import Image from 'next/image';
import { useRouter } from 'next/navigation';
export const DBTypeToImageMapping = (peerType: DBType) => {
  switch (peerType) {
    case DBType.POSTGRES:
      return '/svgs/pg.svg';
    case DBType.SNOWFLAKE:
      return '/svgs/sf.svg';
    case DBType.BIGQUERY:
      return '/svgs/bq.svg';
    case DBType.EVENTHUB_GROUP:
      return '/svgs/ms.svg';
    case DBType.EVENTHUB:
      return '/svgs/ms.svg';
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
      variant='peer'
      style={{
        fontSize: 13,
        padding: '0.5rem',
        borderRadius: '0.6rem',
      }}
      onClick={() => router.push(`/peers/${peerName}`)}
    >
      <Image
        src={DBTypeToImageMapping(peerType)}
        height={15}
        alt={''}
        width={20}
      />
      <Label>{peerName}</Label>
    </Button>
  );
};

export default PeerButton;
