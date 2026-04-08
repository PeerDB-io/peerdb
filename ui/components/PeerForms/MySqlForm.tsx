'use client';

import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import {
  blankSSHConfig,
  sshSetting,
} from '@/app/peers/create/[peerType]/helpers/ssh';
import { useSelectTheme } from '@/app/styles/select';
import InfoPopover from '@/components/InfoPopover';
import {
  AwsIAMAuthConfigType,
  MySqlAuthType,
  MySqlConfig,
  SSHConfig,
} from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { handleFieldChange, handleSSHParam } from './common';

interface MySqlProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: MySqlConfig;
}

export default function MySqlForm({ settings, setter, config }: MySqlProps) {
  const selectTheme = useSelectTheme();
  const [showSSH, setShowSSH] = useState(false);
  const [sshConfig, setSSHConfig] = useState(blankSSHConfig);

  useEffect(() => {
    setter((prev) => ({
      ...prev,
      sshConfig: showSSH ? sshConfig : undefined,
    }));
  }, [sshConfig, setter, showSSH]);

  return (
    <>
      {settings.map((setting, id) => {
        return setting.type === 'switch' ? (
          <RowWithSwitch
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
              <div>
                <Switch
                  onCheckedChange={(val: boolean) =>
                    setting.stateHandler(val, setter)
                  }
                />
                {setting.tips && (
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                )}
              </div>
            }
          />
        ) : setting.type === 'select' &&
          (setting.field !== 'awsAuth.authType' ||
            (config.authType === MySqlAuthType.MYSQL_IAM_AUTH &&
              setting.field === 'awsAuth.authType')) ? (
          <div
            key={id}
            style={{
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
            }}
          >
            <div
              style={{
                flex: 1,
              }}
            >
              <RowWithSelect
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
                    theme={selectTheme}
                  />
                }
              />{' '}
            </div>
            {setting.tips && (
              <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
            )}
          </div>
        ) : (
          (setting.field !== 'awsAuth.authType' &&
            (!setting.field?.startsWith('awsAuth.role.') ||
              (setting.field?.startsWith('awsAuth.role.') &&
                config.awsAuth?.authType ===
                  AwsIAMAuthConfigType.IAM_AUTH_ASSUME_ROLE)) &&
            (!setting.field?.startsWith('awsAuth.staticCredentials.') ||
              (setting.field?.startsWith('awsAuth.staticCredentials.') &&
                config.awsAuth?.authType ===
                  AwsIAMAuthConfigType.IAM_AUTH_STATIC_CREDENTIALS)) &&
            (setting.field !== 'password' ||
              (setting.field?.startsWith('password') &&
                config.authType === MySqlAuthType.MYSQL_PASSWORD)) && (
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
                        handleFieldChange(e, setting, setter)
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
            )) || <div key={id} />
        );
      })}
      <Label
        as='label'
        style={{ marginTop: '1rem', display: 'block' }}
        variant='subheadline'
        colorName='lowContrast'
      >
        SSH Configuration
      </Label>
      <Label>
        You may provide SSH configuration to connect to your MySQL database
        through SSH tunnel.
      </Label>
      <div style={{ width: '50%', display: 'flex', alignItems: 'center' }}>
        <Label variant='subheadline'>Configure SSH Tunnel</Label>
        <Switch onCheckedChange={(state) => setShowSSH(state)} />
      </div>
      {showSSH &&
        sshSetting.map((sshParam, index) => (
          <RowWithTextField
            key={index}
            label={
              <Label>
                {sshParam.label}{' '}
                {!sshParam.optional && (
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
                {sshParam.label === 'SSH Private Key' ? (
                  <TextField
                    variant='simple'
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      handleSSHParam(e, sshParam, setSSHConfig)
                    }
                    style={{ border: 'none' }}
                    type={sshParam.type}
                  />
                ) : (
                  <TextField
                    variant='simple'
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      handleSSHParam(e, sshParam, setSSHConfig)
                    }
                    style={{
                      height: sshParam.type === 'textarea' ? '15rem' : 'auto',
                    }}
                    type={sshParam.type}
                    value={
                      (sshConfig as SSHConfig)[
                        sshParam.label === "Host's Public Key"
                          ? 'hostKey'
                          : (sshParam.label.toLowerCase() as keyof SSHConfig)
                      ] ?? ''
                    }
                  />
                )}
                {sshParam.tips && <InfoPopover tips={sshParam.tips} />}
              </div>
            }
          />
        ))}
    </>
  );
}
