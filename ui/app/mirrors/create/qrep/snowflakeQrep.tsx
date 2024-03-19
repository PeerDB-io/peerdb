'use client';
import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { QRepConfig } from '@/grpc_generated/flow';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { Callout } from '@tremor/react';
import { Dispatch, SetStateAction, useEffect } from 'react';
import { MirrorSetter } from '../../types';
import TableMapping from '../cdc/tablemapping';
import { blankSnowflakeQRepSetting } from '../helpers/common';

interface SnowflakeQRepProps {
  mirrorConfig: QRepConfig;
  setter: MirrorSetter;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

export default function SnowflakeQRepForm({
  mirrorConfig,
  setter,
  rows,
  setRows,
}: SnowflakeQRepProps) {
  useEffect(() => {
    // set defaults
    setter((curr) => ({ ...curr, ...blankSnowflakeQRepSetting }));
  }, [setter]);
  return (
    <>
      {mirrorConfig.sourcePeer?.name ? (
        <div
          style={{ display: 'flex', flexDirection: 'column', rowGap: '1rem' }}
        >
          <Callout title='Note' color='gray'>
            Query replication mirrors with Snowflake source supports only
            overwrite, full-refresh mode.
          </Callout>
          <TableMapping
            sourcePeerName={mirrorConfig.sourcePeer?.name}
            rows={rows}
            setRows={setRows}
            omitAdditionalTablesMapping={new Map<string, string[]>()}
            disableColumnView={true}
            peerType={mirrorConfig.sourcePeer?.type}
          />
        </div>
      ) : (
        <Label as='label' style={{ color: 'gray', fontSize: 15 }}>
          Please select a source and destination peer
        </Label>
      )}
    </>
  );
}
