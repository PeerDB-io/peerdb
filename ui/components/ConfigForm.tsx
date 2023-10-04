'use client';
import { Setting } from '@/app/peers/create/configuration/helpers/common';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { PeerSetter } from '../app/peers/create/configuration/types';

interface ConfigProps {
  settings: Setting[];
  setter: PeerSetter;
}

export default function ConfigForm(props: ConfigProps) {
  const handleFile = (
    file: File,
    setFile: (value: string, setter: PeerSetter) => void
  ) => {
    const reader = new FileReader();
    reader.readAsText(file);
    reader.onload = () => {
      console.log(reader.result);
      setFile(reader.result as string, props.setter);
    };
    reader.onerror = (error) => {
      console.log(error);
    };
  };

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: Setting
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
            label={<Label>{setting.label}</Label>}
            action={
              <TextField
                variant='simple'
                style={
                  setting.type === 'file'
                    ? { border: 'none', height: 'auto' }
                    : { border: 'auto' }
                }
                type={setting.type}
                onChange={(e) => handleChange(e, setting)}
              />
            }
          />
        );
      })}
    </>
  );
}
