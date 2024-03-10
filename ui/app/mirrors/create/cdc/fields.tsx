'use client';
import SelectTheme from '@/app/styles/select';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import ReactSelect from 'react-select';
import { InfoPopover } from '../../../../components/InfoPopover';
import { MirrorSetting } from '../helpers/common';

interface FieldProps {
  setting: MirrorSetting;
  handleChange: (val: string | boolean, setting: MirrorSetting) => void;
  options?: string[];
}

const CDCField = ({ setting, handleChange, options }: FieldProps) => {
  return setting.type === 'switch' ? (
    <RowWithSwitch
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
  ) : setting.type === 'select' ? (
    <RowWithSelect
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
          <div style={{ width: '100%' }}>
            <ReactSelect
              placeholder={`Select a publication`}
              onChange={(val, action) =>
                val && handleChange(val.option, setting)
              }
              options={options?.map((option) => ({ option, label: option }))}
              getOptionLabel={(option) => option.label}
              getOptionValue={(option) => option.option}
              theme={SelectTheme}
            />
          </div>
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
