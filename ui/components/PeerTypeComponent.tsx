'use client';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import Image from 'next/image';
import { DBTypeToImageMapping } from './PeerComponent';

const DBTypeToGoodText = (ptype: DBType) => {
  switch (ptype) {
    case DBType.POSTGRES:
      return 'PostgreSQL';
    case DBType.SNOWFLAKE:
      return 'Snowflake';
    case DBType.EVENTHUB:
      return 'Event Hubs';
    case DBType.EVENTHUB_GROUP:
      return 'Event Hubs';
    case DBType.BIGQUERY:
      return 'BigQuery';
    case DBType.S3:
      return 'AWS S3';
    case DBType.SQLSERVER:
      return 'SQL Server';
    case DBType.MONGO:
      return 'MongoDB';
    case DBType.UNRECOGNIZED:
      return 'Unrecognised';
  }
};

const PeerTypeLabel = ({ ptype }: { ptype: DBType }) => {
  return (
    <div
      style={{
        backgroundColor: 'white',
        color: 'white',
        fontSize: 13,
        display: 'flex',
        alignItems: 'center',
      }}
    >
      <Image
        src={DBTypeToImageMapping(ptype)}
        style={{ height: '1.5rem' }}
        alt=''
      ></Image>
      <Label>{DBTypeToGoodText(ptype)}</Label>
    </div>
  );
};

export default PeerTypeLabel;
