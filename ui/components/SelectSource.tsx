'use client';
import TitleCase from '@/app/utils/titlecase';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button/Button';
import Image from 'next/image';
import Link from 'next/link';
import { DBTypeToImageMapping } from './PeerComponent';

function SourceLabel({ label }: { label: string }) {
  const peerLogo = DBTypeToImageMapping(label);
  return (
    <Button
      as={Link}
      href={`/peers/create/${label}`}
      style={{
        justifyContent: 'space-between',
        padding: '0.5rem',
        backgroundColor: 'white',
        borderRadius: '1rem',
        border: '1px solid rgba(0,0,0,0.1)',
      }}
    >
      <Image
        src={peerLogo}
        alt='peer'
        width={20}
        height={20}
        objectFit='cover'
      />
      <div>{TitleCase(label)}</div>
    </Button>
  );
}

export default function SelectSource() {
  const dbTypes = Object.values(DBType)
    .filter(
      (value): value is string =>
        typeof value === 'string' &&
        (value === 'POSTGRESQL' ||
          value === 'SNOWFLAKE' ||
          value === 'BIGQUERY' ||
          value === 'S3' ||
          value === 'CLICKHOUSE' ||
          value === 'KAFKA' ||
          value === 'EVENTHUBS' ||
          value === 'PUBSUB' ||
          value === 'ELASTICSEARCH')
    )
    .map((value) => ({ label: value, value }));

  dbTypes.push(
    { value: 'POSTGRESQL', label: 'POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'RDS POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'GOOGLE CLOUD POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'AZURE FLEXIBLE POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'TEMBO' },
    { value: 'POSTGRESQL', label: 'CRUNCHY POSTGRES' }
  );

  const gridContainerStyle = {
    display: 'grid',
    gridTemplateColumns: 'repeat(3, 1fr)',
    gap: '20px',
  };

  return (
    <div style={gridContainerStyle}>
      {dbTypes.map((dbtype) => (
        <SourceLabel key={dbtype.label} label={dbtype.label} />
      ))}
    </div>
  );
}
