'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepConfig, QRepWriteType } from '@/grpc_generated/flow';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect } from 'react';
import ReactSelect from 'react-select';
import { InfoPopover } from '../../../../components/InfoPopover';
import { MirrorSetter } from '../../types';
import { MirrorSetting, blankSnowflakeQRepSetting } from '../helpers/common';
import { snowflakeQRepSettings } from '../helpers/qrep';
import QRepQuery from './query';

interface SnowflakeQRepProps {
  mirrorConfig: QRepConfig;
  setter: MirrorSetter;
}

export default function SnowflakeQRepForm({
  mirrorConfig,
  setter,
}: SnowflakeQRepProps) {
  const WriteModes = ['Append', 'Overwrite'].map((value) => ({
    label: value,
    value,
  }));

  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | QRepWriteType | string[] = val;
    if (setting.label.includes('Write Type')) {
      switch (val) {
        case 'Append':
          stateVal = QRepWriteType.QREP_WRITE_MODE_APPEND;
          break;
        case 'Overwrite':
          stateVal = QRepWriteType.QREP_WRITE_MODE_OVERWRITE;
          break;
        default:
          stateVal = QRepWriteType.QREP_WRITE_MODE_APPEND;
          break;
      }
    }
    setting.stateHandler(stateVal, setter);
  };

  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      setting.label === 'Upsert Key Columns' ||
      setting.label === 'Watermark Column'
    ) {
      return false;
    }
    return true;
  };

  useEffect(() => {
    // set defaults
    setter((curr) => ({ ...curr, ...blankSnowflakeQRepSetting }));
  }, [setter]);
  return (
    <>
      <Label>
        Snowflake to PostgreSQL Query Replication currently only supports
        append-only, full-table streaming replication. More features coming
        soon!
      </Label>
      <QRepQuery
        query={mirrorConfig.query}
        setter={(val: string) => {
          setter((curr) => ({
            ...curr,
            query: val,
          }));
        }}
      />
      {mirrorConfig.sourcePeer?.name ? (
        snowflakeQRepSettings.map((setting, id) => {
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
                      checked={mirrorConfig.setupWatermarkTableOnDestination}
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
                    <div style={{ width: '100%' }}>
                      <ReactSelect
                        placeholder='Select a write mode'
                        onChange={(val) =>
                          val && handleChange(val.value, setting)
                        }
                        options={WriteModes}
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
                      defaultValue={setting.default as string}
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
        })
      ) : (
        <Label as='label' style={{ color: 'gray', fontSize: 15 }}>
          Please select a source peer
        </Label>
      )}
    </>
  );
}
