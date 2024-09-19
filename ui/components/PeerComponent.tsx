'use client';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Image from 'next/image';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
export const DBTypeToImageMapping = (peerType: DBType | string) => {
  switch (peerType) {
    case 'AZURE FLEXIBLE POSTGRESQL':
      return '/svgs/azurepg.svg';
    case 'RDS POSTGRESQL':
      return '/svgs/rds.svg';
    case 'GOOGLE CLOUD POSTGRESQL':
      return '/svgs/gcp.svg';
    case 'TEMBO':
      return '/images/tembo.png';
    case 'CRUNCHY POSTGRES':
      return '/images/crunchy.png';
    case 'NEON':
      return '/images/neon.png';
    case 'SUPABASE':
      return '/svgs/supabase-logo-icon.svg';
    case DBType.POSTGRES:
    case 'POSTGRES':
      return '/svgs/pg.svg';
    case DBType.SNOWFLAKE:
    case 'SNOWFLAKE':
      return '/svgs/sf.svg';
    case DBType.BIGQUERY:
    case 'BIGQUERY':
      return '/svgs/bq.svg';
    case DBType.S3:
    case 'S3':
      return '/svgs/aws.svg';
    case 'CLICKHOUSE':
    case DBType.CLICKHOUSE:
      return '/svgs/ch.svg';
    case DBType.EVENTHUBS:
      return '/svgs/ms.svg';
    case DBType.KAFKA:
    case 'KAFKA':
      return '/svgs/kafka.svg';
    case 'CONFLUENT':
      return '/svgs/confluent.svg';
    case 'REDPANDA':
      return '/svgs/redpanda.svg';
    case DBType.PUBSUB:
    case 'PUBSUB':
      return '/svgs/pubsub.svg';
    case 'EVENTHUBS':
      return '/svgs/ms.svg';
    case DBType.ELASTICSEARCH:
    case 'ELASTICSEARCH':
      return '/svgs/elasticsearch.svg';
    default:
      return '/svgs/pg.svg';
  }
};

export default function PeerButton({
  peerName,
  peerType,
}: {
  peerName: string;
  peerType: DBType;
}) {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);

  const handleClick = () => {
    setIsLoading(true);
    router.push(`/peers/${peerName}`);
  };
  return (
    <Button
      variant='peer'
      style={{
        fontSize: 13,
        padding: '0.5rem',
        borderRadius: '0.6rem',
      }}
      onClick={handleClick}
    >
      {isLoading ? (
        <ProgressCircle variant='determinate_progress_circle' />
      ) : (
        <Image
          src={DBTypeToImageMapping(peerType)}
          height={15}
          alt={''}
          width={20}
        />
      )}
      <Label>{peerName}</Label>
    </Button>
  );
}
