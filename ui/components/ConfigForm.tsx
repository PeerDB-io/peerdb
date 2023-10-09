'use client';
import { PeerSetting } from '@/app/peers/create/configuration/helpers/common';
import { PeerSetter } from '@/app/peers/create/configuration/types';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { InfoPopover } from './InfoPopover';

interface ConfigProps {
  settings: PeerSetting[];
  setter: PeerSetter;
}

export default function ConfigForm(props: ConfigProps) {
  const handleFile = (
    file: File,
    setFile: (value: string, setter: PeerSetter) => void
  ) => {
    if (file) {
      const reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        setFile(reader.result as string, props.setter);
      };
      reader.onerror = (error) => {
        console.log(error);
      };
    }
  };

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: PeerSetting
  ) => {
    if (setting.type === 'file') {
      if (e.target.files) handleFile(e.target.files[0], setting.stateHandler);
    } else {
      setting.stateHandler(e.target.value, props.setter);
    }
  };
  return (
    <>
      {props.settings.map((setting, id) => {
        return (
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
                  style={
                    setting.type === 'file'
                      ? { border: 'none', height: 'auto' }
                      : { border: 'auto' }
                  }
                  type={setting.type}
                  defaultValue={setting.default}
                  onChange={(e) => handleChange(e, setting)}
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
