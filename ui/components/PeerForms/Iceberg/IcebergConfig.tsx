'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import {
  CommonConfigSettings,
  FileIoSettings,
  JdbcConfigSettings,
} from '@/app/peers/create/[peerType]/helpers/ice';
import { IcebergConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { useState } from 'react';
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
  return (
    <div
      style={{
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        rowGap: '2rem',
      }}
    >
      <div>
        <Label variant='subheadline'>Common Settings</Label>
        {CommonConfigSettings.map((setting) => (
          <RowWithTextField
            key={setting.label}
            label={<Label as='label'>{setting.label}</Label>}
            action={
              <TextField
                variant='simple'
                type={setting.type}
                onChange={(e) => setting.stateHandler(e.target.value, setter)}
              />
            }
          />
        ))}
      </div>

      <div>
        <Label variant='subheadline'>S3 Settings</Label>
        {FileIoSettings.map((setting) => (
          <RowWithTextField
            key={setting.label}
            label={<Label as='label'>{setting.label}</Label>}
            action={
              <TextField
                variant='simple'
                type={setting.type}
                onChange={(e) => setting.stateHandler(e.target.value, setter)}
              />
            }
          />
        ))}
      </div>

      <div>
        <RowWithSelect
          label={
            <Label as='label' variant='subheadline'>
              Choose specific catalog
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

        {specificCatalog === 'jdbc' &&
          JdbcConfigSettings.map((setting) =>
            setting.type == 'switch' ? (
              <RowWithSwitch
                key={setting.label}
                label={<Label as='label'>{setting.label}</Label>}
                action={
                  <Switch
                    defaultChecked={false}
                    onCheckedChange={(checked) =>
                      setting.stateHandler(checked, setter)
                    }
                  />
                }
              />
            ) : (
              <RowWithTextField
                key={setting.label}
                label={<Label as='label'>{setting.label}</Label>}
                action={
                  <TextField
                    variant='simple'
                    type={setting.type}
                    onChange={(e) =>
                      setting.stateHandler(e.target.value, setter)
                    }
                  />
                }
              />
            )
          )}
      </div>
    </div>
  );
};

export default IcebergConfigForm;
