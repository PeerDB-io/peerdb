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
          <div>
            <Label>
              Refresh interval is the time (in seconds) intervals at which new
              rows will be pulled for replication.
            </Label>
            <RowWithTextField
              label={
                <Label>
                  Refresh Interval
                  <Tooltip
                    style={{ width: '100%' }}
                    content={'This is a required field.'}
                  >
                    <Label colorName='lowContrast' colorSet='destructive'>
                      *
                    </Label>
                  </Tooltip>
                </Label>
              }
              action={
                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'row',
                    alignItems: 'center',
                  }}
                >
                  <TextField
                    variant='simple'
                    type={'number'}
                    defaultValue={mirrorConfig.waitBetweenBatchesSeconds}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      setter((curr) => ({
                        ...curr,
                        waitBetweenBatchesSeconds: e.target.valueAsNumber,
                      }))
                    }
                  />
                </div>
              }
            />
          </div>
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
