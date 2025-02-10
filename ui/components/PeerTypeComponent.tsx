'use client';
import { DBType, dBTypeFromJSON } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import Image from 'next/image';
import { DBTypeToImageMapping } from './PeerComponent';

export function DBTypeToGoodText(ptype?: DBType) {
  switch (dBTypeFromJSON(ptype)) {
    case DBType.POSTGRES:
      return 'PostgreSQL';
    case DBType.MYSQL:
      return 'MySQL';
    case DBType.SNOWFLAKE:
      return 'Snowflake';
    case DBType.EVENTHUBS:
      return 'Event Hubs';
    case DBType.BIGQUERY:
      return 'BigQuery';
    case DBType.S3:
      return 'AWS S3';
    case DBType.SQLSERVER:
      return 'SQL Server';
    case DBType.MONGO:
      return 'MongoDB';
    case DBType.CLICKHOUSE:
      return 'Clickhouse';
    case DBType.KAFKA:
      return 'Kafka';
    case DBType.PUBSUB:
      return 'PubSub';
    case DBType.ELASTICSEARCH:
      return 'Elasticsearch';
    default:
      return 'Unrecognised';
  }
}

export default function PeerTypeLabel({ ptype }: { ptype: DBType }) {
  return (
    <div
      style={{
        fontSize: 13,
        display: 'flex',
        alignItems: 'center',
      }}
    >
      <Image
        src={DBTypeToImageMapping(ptype)}
        height={15}
        alt={''}
        width={20}
      ></Image>
      <Label>{DBTypeToGoodText(ptype)}</Label>
    </div>
  );
}
