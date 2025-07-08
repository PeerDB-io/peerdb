import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import SelectTheme from '@/app/styles/select';
import InfoPopover from '@/components/InfoPopover';
import { handleFieldChange } from '@/components/PeerForms/common';
import { MongoConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import ReactSelect from 'react-select';

interface MongoFormProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: MongoConfig;
}

export default function MongoForm({
  settings,
  setter,
  config,
}: MongoFormProps) {
  return (
    <>
      {settings.map((setting, id) => {
        if (setting.type === 'switch') {
          return (
            <RowWithSwitch
              key={id}
              label={
                <Label>
                  {setting.label}{' '}
                  {!setting.optional && (
                    <Tooltip
                      style={{ width: '100%' }}
                      content='This is a required field.'
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
        }

        if (setting.type === 'select') {
          return (
            <div key={id}>
              <RowWithSelect
                label={
                  <Label>
                    {setting.label}
                    {!setting.optional && (
                      <Tooltip
                        style={{ width: '100%' }}
                        content='This is a required field.'
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
                    <div>
                      <ReactSelect
                        placeholder={setting.placeholder}
                        defaultValue={setting.options?.find(
                          ({ value }) => value === setting.default
                        )}
                        onChange={(val) =>
                          val && setting.stateHandler(val.value, setter)
                        }
                        options={setting.options}
                        theme={SelectTheme}
                      />
                    </div>
                    {setting.tips && (
                      <InfoPopover
                        tips={setting.tips}
                        link={setting.helpfulLink}
                      />
                    )}
                  </div>
                }
              />
            </div>
          );
        }

        return (
          <div key={id}>
            <RowWithTextField
              label={
                <Label>
                  {setting.label}{' '}
                  {!setting.optional && (
                    <Tooltip content='This is a required field.'>
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
                    style={
                      setting.type === 'file'
                        ? { border: 'none', height: 'auto' }
                        : { border: 'auto' }
                    }
                    type={setting.type}
                    defaultValue={setting.default}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      handleFieldChange(e, setting, setter)
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
          </div>
        );
      })}
    </>
  );
}
