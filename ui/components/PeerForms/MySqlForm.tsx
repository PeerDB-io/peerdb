'use client';

import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import SelectTheme from '@/app/styles/select';
import InfoPopover from '@/components/InfoPopover';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import ReactSelect from 'react-select';

interface ConfigProps {
  settings: PeerSetting[];
  setter: PeerSetter;
}

export default function MySqlForm({ settings, setter }: ConfigProps) {
  const handleSwitchChange = (val: string | boolean, setting: PeerSetting) => {
    setting.stateHandler(val, setter);
  };
  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: PeerSetting
  ) => {
    setting.stateHandler(e.target.value, setter);
  };
  return (
    <>
      {settings.map((setting, id) => {
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
                    handleSwitchChange(state, setting)
                  }
                />
                {setting.tips && (
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                )}
              </div>
            }
          />
        ) : setting.type === 'select' ? (
          <RowWithSelect
            key={id}
            label={<Label>{setting.label}</Label>}
            action={
              <ReactSelect
                placeholder={setting.placeholder}
                defaultValue={
                  setting.options &&
                  setting.options.find((x) => x.value === setting.default)
                }
                onChange={(val) =>
                  val && setting.stateHandler(val.value, setter)
                }
                options={setting.options}
                theme={SelectTheme}
              />
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
                    handleTextFieldChange(e, setting)
                  }
                />
                {setting.tips && (
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                )}
              </div>
            }
          />
        );
      })}
    </>
  );
}
