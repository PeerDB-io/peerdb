'use client';
import { DBType } from '@/grpc_generated/peers';
import Image from 'next/image';
import { Dispatch, SetStateAction } from 'react';
import ReactSelect from 'react-select';
import { DBTypeToImageMapping } from './PeerComponent';

interface SelectSourceProps {
  peerType: string;
  setPeerType: Dispatch<SetStateAction<string>>;
}

function SourceLabel({ value }: { value: string }) {
  const peerLogo = DBTypeToImageMapping(value);
  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <Image src={peerLogo} alt='peer' height={15} width={15} />
      <div style={{ marginLeft: 10 }}>{value}</div>
    </div>
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
        (value === 'POSTGRES' ||
          value === 'SNOWFLAKE' ||
          value === 'BIGQUERY' ||
          value === 'S3')
    )
    .map((value) => ({ label: value, value }));

  return (
    <ReactSelect
      placeholder='Select a source'
      options={dbTypes}
      defaultValue={dbTypes.find((opt) => opt.value === peerType)}
      onChange={(val, _) => val && setPeerType(val.value)}
      formatOptionLabel={SourceLabel}
    />
  );
}
