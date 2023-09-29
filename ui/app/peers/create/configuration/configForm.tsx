'use client';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { PeerSetter } from './types';

interface Setting {
  label: string;
  stateHandler: (value: string, setter: PeerSetter) => void;
  type?: string;
}

interface ConfigProps {
  settings: Setting[];
  setter: PeerSetter;
}

export default function PgConfig(props: ConfigProps) {
  return (
    <>
      {props.settings.map((setting, id) => {
        return (
          <RowWithTextField
            key={id}
            label={<Label as='label'>{setting.label}</Label>}
            action={
              <TextField
                variant='simple'
                type={setting.type}
                onChange={(e) =>
                  setting.stateHandler(e.target.value, props.setter)
                }
              />
            }
          />
        );
      })}
    </>
  );
}
