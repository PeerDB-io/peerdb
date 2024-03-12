'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { InfoPopover } from '../InfoPopover';
interface ConfigProps {
  settings: PeerSetting[];
  setter: PeerSetter;
}

export default function ClickhouseForm({ settings, setter }: ConfigProps) {
  const S3Labels = ['S3 Path', 'Access Key ID', 'Secret Access Key', 'Region'];
  const handleChange = (val: string | boolean, setting: PeerSetting) => {
    setting.stateHandler(val, setter);
  };

  return (
    <>
      {settings
        .filter((setting) => !S3Labels.includes(setting.label))
        .map((setting, id) => {
          return setting.type == 'switch' ? (
            <RowWithSwitch
              key={id}
              label={
                <Label>
                  {setting.label}{' '}
                  {!setting.optional && (
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
                <div>
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
                  {setting.label}{' '}
                  {!setting.optional && (
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
          );
        })}
      <Label
        as='label'
        style={{ marginTop: '1rem' }}
        variant='subheadline'
        colorName='lowContrast'
      >
        Transient S3 Stage
      </Label>
      {settings
        .filter((setting) => S3Labels.includes(setting.label))
        .map((setting, id) => (
          <RowWithTextField
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
                <TextField
                  variant='simple'
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleChange(e.target.value, setting)
                  }
                  type={setting.type}
                />
                {setting.tips && <InfoPopover tips={setting.tips} />}
              </div>
            }
          />
        ))}
    </>
  );
}
