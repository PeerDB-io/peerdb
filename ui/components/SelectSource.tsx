'use client';
import TitleCase from '@/app/utils/titlecase';
import { Button } from '@/lib/Button/Button';
import Image from 'next/image';
import Link from 'next/link';
import { DBTypeToImageMapping } from './PeerComponent';

function SourceLabel({ label }: { label: string }) {
  const peerLogo = DBTypeToImageMapping(label);
  return (
    <Button
      as={Link}
      href={`/ peers / create / ${label}`}
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

const dbTypes = [
  [
    'Postgres',
    'POSTGRESQL',
    'RDS POSTGRESQL',
    'GOOGLE CLOUD POSTGRESQL',
    'AZURE FLEXIBLE POSTGRESQL',
    'TEMBO',
    'CRUNCHY POSTGRES',
  ],
  ['Warehouses', 'SNOWFLAKE', 'BIGQUERY', 'S3', 'CLICKHOUSE', 'ELASTICSEARCH'],
  ['Queues', 'KAFKA', 'EVENTHUBS', 'PUBSUB'],
];

const gridContainerStyle = {
  display: 'flex',
  gap: '20px',
  flexWrap: 'wrap',
  border: 'solid #18794e',
  borderRadius: '20px',
  position: 'relative',
  padding: '20px',
  marginTop: '20px',
} as const;
const gridHeaderStyle = {
  position: 'absolute',
  top: '-15px',
  height: '30px',
  display: 'flex',
  alignItems: 'center',
  color: '#fff',
  backgroundColor: '#18794e',
  borderRadius: '15px',
  marginLeft: '10px',
  paddingLeft: '10px',
  paddingRight: '10px',
} as const;

export default function SelectSource() {
  return dbTypes.map(([category, ...items]) => (
    <div key={category} style={gridContainerStyle}>
      <div style={gridHeaderStyle}>{category}</div>
      {items.map((item) => (
        <SourceLabel key={item} label={item} />
      ))}
    </div>
  ));
}
