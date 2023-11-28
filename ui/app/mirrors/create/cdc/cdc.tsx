'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepSyncMode } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction } from 'react';
import { InfoPopover } from '../../../../components/InfoPopover';
import { CDCConfig, MirrorSetter, TableMapRow } from '../../../dto/MirrorsDTO';
import { MirrorSetting } from '../helpers/common';
import TableMapping from './tablemapping';

interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: CDCConfig;
  setter: MirrorSetter;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

export const defaultSyncMode = (dtype: DBType | undefined) => {
  switch (dtype) {
    case DBType.POSTGRES:
      return 'Copy with Binary';
    case DBType.SNOWFLAKE:
    case DBType.BIGQUERY:
    case DBType.S3:
      return 'AVRO';
    default:
      return 'Copy with Binary';
  }
};

export default function CDCConfigForm({
  settings,
  mirrorConfig,
  setter,
  rows,
  setRows,
}: MirrorConfigProps) {
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | QRepSyncMode = val;
    setting.stateHandler(stateVal, setter);
  };

  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      (label.includes('snapshot') && mirrorConfig.doInitialCopy !== true) ||
      (label.includes('staging path') &&
        defaultSyncMode(mirrorConfig.destination?.type) !== 'AVRO')
    ) {
      return false;
    }
    return true;
  };

  if (mirrorConfig.source != undefined && mirrorConfig.destination != undefined)
    return (
      <>
        <TableMapping
          sourcePeerName={mirrorConfig.source?.name}
          rows={rows}
          setRows={setRows}
          peerType={mirrorConfig.destination?.type}
        />
        {settings.map((setting, id) => {
          return (
            paramDisplayCondition(setting) &&
            (setting.type === 'switch' ? (
              <RowWithSwitch
                key={id}
                label={<Label>{setting.label}</Label>}
                action={
                  <div
                    style={{
                      display: 'flex',
                      flexDirection: 'row',
                      alignItems: 'center',
                    }}
                  >
                    <Switch
                      onCheckedChange={(state: boolean) =>
                        handleChange(state, setting)
                      }
                    />
                    {setting.tips && (
                      <InfoPopover
                        tips={setting.tips}
                        link={setting.helpfulLink}
                      />
                    )}
                  </div>
                }
              />
            ) : (
              <RowWithTextField
                key={id}
                label={
                  <Label>
                    {setting.label}
                    {RequiredIndicator(setting.required)}
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
                      type={setting.type}
                      defaultValue={setting.default}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                        handleChange(e.target.value, setting)
                      }
                    />
                    {setting.tips && (
                      <InfoPopover
                        tips={setting.tips}
                        link={setting.helpfulLink}
                      />
                    )}
                  </div>
                }
              />
            ))
          );
        })}
      </>
    );
}
