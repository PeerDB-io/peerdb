'use client';
import TitleCase from '@/app/utils/titlecase';
import { DBType } from '@/grpc_generated/peers';
import Image from 'next/image';
import { DBTypeToImageMapping } from './PeerComponent';

interface SelectSourceProps {}

function SourceLabel({ value, label }: { value: string; label: string }) {
  const peerLogo = DBTypeToImageMapping(label);
  return (
    <a
      style={{ display: 'flex', alignItems: 'center' }}
      href={`/peers/create/${label}`}
    >
      <Image src={peerLogo} alt='peer' height={15} width={15} />
      <div style={{ marginLeft: 10 }}>{TitleCase(label)}</div>
    </a>
  );
}

export default function SelectSource({
  peerType,
  setPeerType,
}: SelectSourceProps) {
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
  return (
    <div style={{ columns: 2 }}>
      {dbTypes.map((dbtype) => (
        <SourceLabel
          key={dbtype.label}
          value={dbtype.value}
          label={dbtype.label}
        />
      ))}
    </div>
  );
}
