'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import {
  blankSSHConfig,
  sshSetting,
} from '@/app/peers/create/[peerType]/helpers/ssh';
import InfoPopover from '@/components/InfoPopover';
import { CockroachDBConfig, SSHConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import { handleFieldChange, handleSSHParam } from './common';

interface CockroachDBProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: CockroachDBConfig;
}

export default function CockroachDBForm({
  settings,
  setter,
  config,
}: CockroachDBProps) {
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
      {settings.map((setting, index) => {
        if (setting.type === 'switch') {
          return (
            <RowWithSwitch
              key={index}
              label={
                <Label>
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
                <div>
                  <Switch
                    onCheckedChange={(val: boolean) =>
                      setting.stateHandler(val, setter)
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
          );
        } else if (setting.type === 'file') {
          return (
            <RowWithTextField
              key={index}
              label={
                <Label>
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
                  {setting.tips && (
                    <InfoPopover
                      tips={setting.tips}
                      link={setting.helpfulLink}
                    />
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
                  <Tooltip content={setting.tips || setting.label}>
                    <TextField
                      variant='simple'
                      type='file'
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                        handleFieldChange(e, setting, setter)
                      }
                    />
                  </Tooltip>
                </div>
              }
            />
          );
        } else {
          return (
            <RowWithTextField
              key={index}
              label={
                <Label>
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
                  {setting.tips && (
                    <InfoPopover
                      tips={setting.tips}
                      link={setting.helpfulLink}
                    />
                  )}
                </Label>
              }
              action={
                <TextField
                  variant='simple'
                  type={setting.type ?? 'text'}
                  defaultValue={
                    setting.field && config
                      ? (config as any)[setting.field]
                      : (setting.default as string)
                  }
                  placeholder={setting.placeholder}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleFieldChange(e, setting, setter)
                  }
                />
              }
            />
          );
        }
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
        You may provide SSH configuration to connect to your CockroachDB
        database through SSH tunnel.
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
