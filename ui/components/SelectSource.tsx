'use client';
import { DBType } from '@/grpc_generated/peers';
import { Select, SelectItem } from '@/lib/Select';
import { Dispatch, SetStateAction } from 'react';

interface SelectSourceProps {
  peerType: string;
  setPeerType: Dispatch<SetStateAction<string>>;
}

export default function SelectSource({ setPeerType }: SelectSourceProps) {
  const dbTypes: string[] = Object.values(DBType).filter(
    (value): value is string =>
      typeof value === 'string' &&
      (value === 'POSTGRES' || value === 'SNOWFLAKE')
  );
  return (
    <Select
      placeholder='Select a source'
      id='source'
      onValueChange={(val) => setPeerType(val)}
    >
      {dbTypes.map((dbType, id) => {
        return (
          <SelectItem key={id} value={dbType}>
            {dbType}
          </SelectItem>
        );
      })}
    </Select>
  );
}
