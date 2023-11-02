'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepConfig, QRepSyncMode, QRepWriteType } from '@/grpc_generated/flow';
import { DBType, Peer } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Select, SelectItem } from '@/lib/Select';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { InfoPopover } from '../../../components/InfoPopover';
import { MirrorSetter } from '../../dto/MirrorsDTO';
import { defaultSyncMode } from './cdc';
import { MirrorSetting } from './helpers/common';
interface QRepConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: QRepConfig;
  peers: Peer[];
  setter: MirrorSetter;
  xmin?: boolean;
}

export default function QRepConfigForm(props: QRepConfigProps) {
  const setToDefault = (setting: MirrorSetting) => {
    const destinationPeerType = props.mirrorConfig.destinationPeer?.type;
    return (
      setting.label.includes('Sync') &&
      (destinationPeerType === DBType.POSTGRES ||
        destinationPeerType === DBType.SNOWFLAKE)
    );
  };

  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal:
      | string
      | boolean
      | Peer
      | QRepSyncMode
      | QRepWriteType
      | string[] = val;
    if (setting.label.includes('Peer')) {
      stateVal = props.peers.find((peer) => peer.name === val)!;
      if (setting.label === 'Destination Peer') {
        if (stateVal.type === DBType.POSTGRES) {
          props.setter((curr) => {
            return {
              ...curr,
              syncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
            };
          });
        } else if (stateVal.type === DBType.SNOWFLAKE) {
          props.setter((curr) => {
            return {
              ...curr,
              syncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
            };
          });
        }
      }
    } else if (setting.label.includes('Sync Mode')) {
      stateVal =
        val === 'AVRO'
          ? QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO
          : QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    } else if (setting.label.includes('Write Type')) {
      switch (val) {
        case 'Upsert':
          stateVal = QRepWriteType.QREP_WRITE_MODE_UPSERT;
          break;
        case 'Overwrite':
          stateVal = QRepWriteType.QREP_WRITE_MODE_OVERWRITE;
          break;
        default:
          stateVal = QRepWriteType.QREP_WRITE_MODE_APPEND;
          break;
      }
    } else if (setting.label === 'Upsert Key Columns') {
      const columns = val as string;
      stateVal = columns.split(',').map((item) => item.trim());
    }
    setting.stateHandler(stateVal, props.setter);
  };
  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      (label.includes('upsert') &&
        props.mirrorConfig.writeMode?.writeType !=
          QRepWriteType.QREP_WRITE_MODE_UPSERT) ||
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
                    disabled={setToDefault(setting)}
                    value={
                      setToDefault(setting)
                        ? defaultSyncMode(
                            props.mirrorConfig.destinationPeer?.type
                          )
                        : undefined
                    }
                  >
                    {(setting.label.includes('Peer')
                      ? (props.peers ?? []).map((peer) => peer.name)
                      : setting.label.includes('Sync')
                      ? ['AVRO', 'Copy with Binary']
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
