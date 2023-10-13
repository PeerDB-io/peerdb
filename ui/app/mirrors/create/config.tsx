'use client';
import { QRepSyncMode } from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { InfoPopover } from '../../../components/InfoPopover';
import { MirrorConfig, MirrorSetter } from '../types';
import { MirrorSetting } from './helpers/common';

interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: MirrorConfig;
  peers: Peer[];
  setter: MirrorSetter;
}

export default function MirrorConfig(props: MirrorConfigProps) {
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | Peer | QRepSyncMode = val;
    if (setting.label.includes('Peer')) {
      stateVal = props.peers.find((peer) => peer.name === val)!;
    } else if (setting.label.includes('Sync Mode')) {
      stateVal =
        val === 'avro'
          ? QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO
          : QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    }
    setting.stateHandler(stateVal, props.setter);
  };
  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      (label.includes('snapshot') &&
        props.mirrorConfig.doInitialCopy !== true) ||
      (label.includes('snapshot staging') &&
        props.mirrorConfig.snapshotSyncMode?.toString() !== '1') ||
      (label.includes('cdc staging') &&
        props.mirrorConfig.cdcSyncMode?.toString() !== '1')
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
              label={<Label>{setting.label}</Label>}
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
                      setting.label.includes('Peer') ? 'a peer' : 'a sync mode'
                    }`}
                    onValueChange={(val) => handleChange(val, setting)}
                  >
                    {(setting.label.includes('Peer')
                      ? props.peers?.map((peer) => peer.name)
                      : ['avro', 'sql']
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
                    onChange={(e) => handleChange(e.target.value, setting)}
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
