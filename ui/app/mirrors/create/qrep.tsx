'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import {
  QRepSyncMode,
  QRepWriteMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { Dispatch, SetStateAction } from 'react';
import { InfoPopover } from '../../../components/InfoPopover';
import { MirrorSetter, QREPConfig } from '../types';
import { MirrorSetting } from './helpers/common';
interface QRepConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: QREPConfig;
  peers: Peer[];
  setter: MirrorSetter;
  writeMode: QRepWriteMode;
  WriteModeSetter: Dispatch<SetStateAction<QRepWriteMode>>;
  xmin?: boolean;
}

export default function QRepConfigForm(props: QRepConfigProps) {
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | Peer | QRepSyncMode | QRepWriteType = val;
    if (setting.label.includes('Peer')) {
      stateVal = props.peers.find((peer) => peer.name === val)!;
    } else if (setting.label.includes('Sync Mode')) {
      stateVal =
        val === 'avro'
          ? QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO
          : QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    } else if (setting.label.includes('Write Mode')) {
      if (val === 'Upsert') {
        props.WriteModeSetter({
          writeType: QRepWriteType.QREP_WRITE_MODE_UPSERT,
          upsertKeyColumns: [],
        });
      }
      return;
    }
    setting.stateHandler(stateVal, props.setter);
  };
  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      (label.includes('upsert') &&
        props.writeMode.writeType != QRepWriteType.QREP_WRITE_MODE_UPSERT) ||
      (label.includes('staging') &&
        props.mirrorConfig.syncMode?.toString() !== '1') ||
      (label.includes('watermark column') && props.xmin) ||
      (label.includes('initial copy') && props.xmin)
    ) {
      return false;
    }
    return true;
  };

  return (
    <>
      {props.settings.map((setting, id) => {
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
                    checked={
                      setting.label.includes('Create Destination')
                        ? props.mirrorConfig.setupWatermarkTableOnDestination
                        : props.mirrorConfig.initialCopyOnly
                    }
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
                  <Select
                    placeholder={`Select ${
                      setting.label.includes('Peer') ? 'a peer' : 'a mode'
                    }`}
                    onValueChange={(val) => handleChange(val, setting)}
                  >
                    {(setting.label.includes('Peer')
                      ? (props.peers ?? []).map((peer) => peer.name)
                      : setting.label.includes('Sync')
                      ? ['avro', 'sql']
                      : ['Append', 'Upsert', 'Overwrite']
                    ).map((item, id) => {
                      return (
                        <SelectItem key={id} value={item.toString()}>
                          {item.toString()}
                        </SelectItem>
                      );
                    })}
                  </Select>
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
                  {setting.required && (
                    <Tooltip
                      style={{ width: '100%' }}
                      content={'This is a required field.'}
                    >
                      <Label colorName='lowContrast' colorSet='destructive'>
                        *
                      </Label>
                    </Tooltip>
                  )}
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
