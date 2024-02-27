'use client';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepConfig, QRepWriteType } from '@/grpc_generated/flow';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { InfoPopover } from '../../../../components/InfoPopover';
import { MirrorSetter } from '../../types';
import { fetchAllTables } from '../handlers';
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
  const [sourceTables, setSourceTables] = useState<
    { value: string; label: string }[]
  >([]);
  const [loading, setLoading] = useState(false);
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

  const handleSourceChange = (val: string, setting: MirrorSetting) => {
    setter((curr) => ({
      ...curr,
      destinationTableIdentifier: val.toLowerCase(),
    }));
    handleChange(val, setting);
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
    setLoading(true);
    fetchAllTables(
      mirrorConfig.sourcePeer?.name ?? '',
      mirrorConfig.sourcePeer?.type
    ).then((tables) => {
      setSourceTables(tables?.map((table) => ({ value: table, label: table })));
      setLoading(false);
    });
  }, [mirrorConfig.sourcePeer]);

  useEffect(() => {
    // set defaults
    setter((curr) => ({ ...curr, ...blankSnowflakeQRepSetting }));
  }, [setter]);
  return (
    <>
      <Label>
        Snowflake to PostgreSQL Query Replication currently only supports
        full-table streaming replication with overwrite mode. More features
        coming soon!
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
                      {setting.label.includes('Write') ? (
                        <ReactSelect
                          isDisabled={true}
                          placeholder='Select a write mode'
                          value={{ value: 'Overwrite', label: 'Overwrite' }}
                        />
                      ) : (
                        <ReactSelect
                          placeholder={'Select a table'}
                          onChange={(val, action) =>
                            val && handleSourceChange(val.value, setting)
                          }
                          isLoading={loading}
                          options={sourceTables}
                        />
                      )}
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
                      defaultValue={
                        setting.label === 'Destination Table Name'
                          ? mirrorConfig.destinationTableIdentifier
                          : (setting.default as string)
                      }
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
