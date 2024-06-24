'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import {
  CommonConfigSettings,
  FileIoSettings,
  JdbcConfigSettings,
  handleHiveSelection,
} from '@/app/peers/create/[peerType]/helpers/ice';
import { InfoPopover } from '@/components/InfoPopover';
import { IcebergConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip/Tooltip';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';

interface IcebergConfigProps {
  icebergConfig: IcebergConfig;
  setter: PeerSetter;
}

const IcebergConfigForm = ({ icebergConfig, setter }: IcebergConfigProps) => {
  const specificCatalogOptions = [
    { value: 'hive', label: 'Hive' },
    { value: 'jdbc', label: 'JDBC' },
  ];

  const [specificCatalog, setSpecificCatalog] = useState<'hive' | 'jdbc'>(
    'jdbc'
  );

  useEffect(() => {
    if (specificCatalog === 'hive') {
      handleHiveSelection(setter);
    }
  }, [specificCatalog, setter]);

  return (
    <div
      style={{
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        rowGap: '2.5rem',
      }}
    >
      <div>
        <Label variant='subheadline'>Common Settings</Label>
        {CommonConfigSettings.map((setting) => (
          <RowWithTextField
            key={setting.label}
            label={
              <Label as='label'>
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
                  onChange={(e) => setting.stateHandler(e.target.value, setter)}
                />
                {setting.tips && (
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                )}
              </div>
            }
          />
        ))}
      </div>

      <div>
        <Label variant='subheadline'>S3 Settings</Label>
        {FileIoSettings.map((setting) =>
          setting.type == 'switch' ? (
            <RowWithSwitch
              key={setting.label}
              label={<Label as='label'>{setting.label}</Label>}
              action={
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  <Switch
                    defaultChecked={false}
                    onCheckedChange={(checked) =>
                      setting.stateHandler(checked, setter)
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
          ) : (
            <RowWithTextField
              key={setting.label}
              label={
                <Label as='label'>
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
                    onChange={(e) =>
                      setting.stateHandler(e.target.value, setter)
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
          )
        )}
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', rowGap: '2rem' }}>
        <RowWithSelect
          label={
            <Label as='label' variant='subheadline'>
              Choose Iceberg Catalog Type
              <Tooltip
                style={{ width: '100%' }}
                content={'This is a required field.'}
              >
                <Label colorName='lowContrast' colorSet='destructive'>
                  *
                </Label>
              </Tooltip>
            </Label>
          }
          action={
            <ReactSelect
              options={specificCatalogOptions}
              value={specificCatalogOptions.find(
                (option) => option.value === specificCatalog
              )}
              onChange={(option) => {
                if (option) {
                  setSpecificCatalog(option.value as 'hive' | 'jdbc');
                }
              }}
            />
          }
        />

        {specificCatalog === 'jdbc' && (
          <div>
            <Label variant='subheadline'>JDBC Settings</Label>
            {JdbcConfigSettings.map((setting) =>
              setting.type == 'switch' ? (
                <RowWithSwitch
                  key={setting.label}
                  label={<Label as='label'>{setting.label}</Label>}
                  action={
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <Switch
                        defaultChecked={false}
                        onCheckedChange={(checked) =>
                          setting.stateHandler(checked, setter)
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
              ) : (
                <RowWithTextField
                  key={setting.label}
                  label={
                    <Label as='label'>
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
                        onChange={(e) =>
                          setting.stateHandler(e.target.value, setter)
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
              )
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default IcebergConfigForm;
