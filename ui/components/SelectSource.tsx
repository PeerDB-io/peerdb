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

function SourceValue() {}

export default function SelectSource({
  peerType,
  setPeerType,
}: SelectSourceProps) {
  const dbTypes: string[] = Object.values(DBType)
    .filter(
      (value): value is string =>
        typeof value === 'string' &&
        (value === 'POSTGRES' || value === 'SNOWFLAKE' || value === 'BIGQUERY')
    )
    .map((value) => ({ value }));

  return (
    <ReactSelect
      placeholder='Select a source'
      options={dbTypes}
      defaultInputValue={peerType}
      onValueChange={(val) => setPeerType(val)}
    />
  );
}
