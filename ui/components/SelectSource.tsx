'use client';
import SelectTheme from '@/app/styles/select';
import TitleCase from '@/app/utils/titlecase';
import { DBType } from '@/grpc_generated/peers';
import Image from 'next/image';
import { Dispatch, SetStateAction } from 'react';
import ReactSelect from 'react-select';
import { DBTypeToImageMapping } from './PeerComponent';

interface SelectSourceProps {
  peerType: string;
  setPeerType: Dispatch<SetStateAction<string>>;
}

function SourceLabel({ value, label }: { value: string; label: string }) {
  const peerLogo = DBTypeToImageMapping(label);
  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <Image src={peerLogo} alt='peer' height={15} width={15} />
      <div style={{ marginLeft: 10 }}>{TitleCase(label)}</div>
    </div>
  );
}

export default function SelectSource({
  peerType,
  setPeerType,
}: SelectSourceProps) {
  let dbTypes = Object.values(DBType)
    .filter(
      (value): value is string =>
        typeof value === 'string' &&
        (value === 'POSTGRESQL' ||
          value === 'SNOWFLAKE' ||
          value === 'BIGQUERY' ||
          value === 'S3' ||
          value === 'CLICKHOUSE')
    )
    .map((value) => ({ label: value, value }));

  dbTypes.push(
    { value: 'POSTGRESQL', label: 'POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'RDS POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'GOOGLE CLOUD POSTGRESQL' },
    { value: 'POSTGRESQL', label: 'AZURE FLEXIBLE POSTGRESQL' }
  );
  return (
    <ReactSelect
      className='w-full'
      placeholder='Select a source'
      options={dbTypes}
      defaultValue={dbTypes.find((opt) => opt.label === peerType)}
      onChange={(val, _) => val && setPeerType(val.label)}
      formatOptionLabel={SourceLabel}
      theme={SelectTheme}
    />
  );
}
