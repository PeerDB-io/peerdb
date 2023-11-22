'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepSyncMode } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Dispatch, SetStateAction } from 'react';
import ReactSelect from 'react-select';
import { InfoPopover } from '../../../components/InfoPopover';
import { CDCConfig, MirrorSetter, TableMapRow } from '../../dto/MirrorsDTO';
import { MirrorSetting } from './helpers/common';
import TableMapping from './tablemapping';

interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: CDCConfig;
  setter: MirrorSetter;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

const SyncModeOptions = ['AVRO', 'Copy with Binary'].map((value) => ({
  label: value,
  value,
}));

export const defaultSyncMode = (dtype: DBType | undefined) => {
  switch (dtype) {
    case DBType.POSTGRES:
      return 'Copy with Binary';
    case DBType.SNOWFLAKE:
    case DBType.BIGQUERY:
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
  const setToDefault = (setting: MirrorSetting) => {
    const destinationPeerType = mirrorConfig.destination?.type;
    return (
      setting.label.includes('Sync') &&
      (destinationPeerType === DBType.POSTGRES ||
        destinationPeerType === DBType.SNOWFLAKE)
    );
  };
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | QRepSyncMode = val;
    if (setting.label.includes('Sync Mode')) {
      stateVal =
        val === 'AVRO'
          ? QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO
          : QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    }
    setting.stateHandler(stateVal, setter);
  };
  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      (label.includes('snapshot') && mirrorConfig.doInitialCopy !== true) ||
      (label.includes('snapshot staging') &&
        mirrorConfig.snapshotSyncMode?.toString() !== '1') ||
      (label.includes('cdc staging') &&
        mirrorConfig.cdcSyncMode?.toString() !== '1')
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
            ) : setting.type === 'select' ? (
              <RowWithSelect
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
                    <ReactSelect
                      placeholder='Select a sync mode'
                      onChange={(val, action) =>
                        val && handleChange(val.value, setting)
                      }
                      isDisabled={setToDefault(setting)}
                      value={
                        setToDefault(setting)
                          ? SyncModeOptions.find(
                              (opt) =>
                                opt.value ===
                                defaultSyncMode(mirrorConfig.destination?.type)
                            )
                          : undefined
                      }
                      options={SyncModeOptions}
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
