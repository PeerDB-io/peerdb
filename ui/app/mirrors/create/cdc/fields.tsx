'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { InfoPopover } from '../../../../components/InfoPopover';
import { MirrorSetting } from '../helpers/common';

interface FieldProps {
  setting: MirrorSetting;
  handleChange: (val: string | boolean, setting: MirrorSetting) => void;
}

const CDCField = ({ setting, handleChange }: FieldProps) => {
  return setting.type === 'switch' ? (
    <RowWithSwitch
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
            defaultChecked={setting.default as boolean}
            onCheckedChange={(state: boolean) => handleChange(state, setting)}
          />
          {setting.tips && (
            <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
          )}
        </div>
      }
    />
  ) : (
    <RowWithTextField
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
            defaultValue={setting.default as string}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              handleChange(e.target.value, setting)
            }
          />
          {setting.tips && (
            <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
          )}
        </div>
      }
    />
  );
};

export default CDCField;
