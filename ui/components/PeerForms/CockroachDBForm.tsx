import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import InfoPopover from '@/components/InfoPopover';
import { CockroachDBConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { handleFieldChange } from './common';

interface CockroachDBProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: CockroachDBConfig;
}

export default function CockroachDBForm({
  settings,
  setter,
  config,
}: CockroachDBProps) {
  return (
    <>
      {settings.map((setting, index) => {
        if (setting.type === 'switch') {
          return (
            <RowWithSwitch
              key={index}
              label={
                <Label>
                  {setting.label}
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
                    defaultChecked={setting.default as boolean}
                    onCheckedChange={(val: boolean) =>
                      setting.stateHandler(val, setter)
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
        } else if (setting.type === 'file') {
          return (
            <RowWithTextField
              key={index}
              label={
                <Label>
                  {setting.label}
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
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
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
                  <Tooltip content={setting.tips || setting.label}>
                    <TextField
                      variant='simple'
                      type='file'
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                        handleFieldChange(e, setting, setter)
                      }
                    />
                  </Tooltip>
                </div>
              }
            />
          );
        } else {
          return (
            <RowWithTextField
              key={index}
              label={
                <Label>
                  {setting.label}
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
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                </Label>
              }
              action={
                <TextField
                  variant='simple'
                  type={setting.type ?? 'text'}
                  defaultValue={
                    setting.field && config
                      ? (config as any)[setting.field]
                      : (setting.default as string)
                  }
                  placeholder={setting.placeholder}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleFieldChange(e, setting, setter)
                  }
                />
              }
            />
          );
        }
      })}
    </>
  );
}
