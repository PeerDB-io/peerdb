'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepSyncMode } from '@/grpc_generated/flow';
import { DBType, Peer } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { InfoPopover } from '../../../components/InfoPopover';
import { CDCConfig, MirrorSetter } from '../../dto/MirrorsDTO';
import { MirrorSetting } from './helpers/common';
interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: CDCConfig;
  peers: Peer[];
  setter: MirrorSetter;
}

export default function CDCConfigForm(props: MirrorConfigProps) {
  const defaultSyncMode = (
    dtype: DBType | undefined,
    setting: MirrorSetting
  ) => {
    switch (dtype) {
      case DBType.POSTGRES:
        return 'Copy with Binary';
      case DBType.SNOWFLAKE:
        return 'AVRO';
      default:
        return 'Copy with Binary';
    }
  };

  const setToDefault = (setting: MirrorSetting) => {
    const destinationPeerType = props.mirrorConfig.destination?.type;
    return (
      setting.label.includes('Sync') &&
      (destinationPeerType === DBType.POSTGRES ||
        destinationPeerType === DBType.SNOWFLAKE)
    );
  };

  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | Peer | QRepSyncMode = val;
    if (setting.label.includes('Peer')) {
      stateVal = props.peers.find((peer) => peer.name === val)!;
      if (setting.label === 'Destination Peer') {
        if (stateVal.type === DBType.POSTGRES) {
          props.setter((curr) => {
            return {
              ...curr,
              cdcSyncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
              snapshotSyncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
            };
          });
        } else if (stateVal.type === DBType.SNOWFLAKE) {
          props.setter((curr) => {
            return {
              ...curr,
              cdcSyncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
              snapshotSyncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
            };
          });
        }
      }
    } else if (setting.label.includes('Sync Mode')) {
      stateVal =
        val === 'AVRO'
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
                      setting.label.includes('Peer') ? 'a peer' : 'a sync mode'
                    }`}
                    onValueChange={(val) => handleChange(val, setting)}
                    disabled={setToDefault(setting)}
                    value={
                      setToDefault(setting)
                        ? defaultSyncMode(
                            props.mirrorConfig.destination?.type,
                            setting
                          )
                        : undefined
                    }
                  >
                    {(setting.label.includes('Peer')
                      ? (props.peers ?? []).map((peer) => peer.name)
                      : ['AVRO', 'Copy with Binary']
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
