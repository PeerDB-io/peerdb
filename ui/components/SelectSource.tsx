'use client';
import { DBType } from '@/grpc_generated/peers';
import { Select, SelectItem } from '@/lib/Select';
import Image from 'next/image';
import { Dispatch, SetStateAction } from 'react';
import { DBTypeToImageMapping } from './PeerComponent';

interface SelectSourceProps {
  peerType: string;
  setPeerType: Dispatch<SetStateAction<string>>;
}

export default function SelectSource({ setPeerType }: SelectSourceProps) {
  const dbTypes: string[] = Object.values(DBType).filter(
    (value): value is string =>
      typeof value === 'string' &&
      (value === 'POSTGRES' || value === 'SNOWFLAKE' || value === 'BIGQUERY')
  );

  return (
    <Select
      placeholder='Select a source'
      id='source'
      onValueChange={(val) => setPeerType(val)}
    >
      {dbTypes.map((dbType, id) => {
        const peerLogo = DBTypeToImageMapping(dbType);
        return (
          <SelectItem key={id} value={dbType}>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <Image src={peerLogo} alt='peer' height={15} width={15} />

              <div style={{ marginLeft: 10 }}>{dbType}</div>
            </div>
          </SelectItem>
        );
      })}
    </Select>
  );
}
