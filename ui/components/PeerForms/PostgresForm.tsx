'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import {
  blankSSHConfig,
  sshSetting,
} from '@/app/peers/create/[peerType]/helpers/pg';
import { SSHConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import { InfoPopover } from '../InfoPopover';
interface ConfigProps {
  settings: PeerSetting[];
  setter: PeerSetter;
}

export default function PostgresForm({ settings, setter }: ConfigProps) {
  const [showSSH, setShowSSH] = useState<boolean>(false);
  const [sshConfig, setSSHConfig] = useState<SSHConfig>(blankSSHConfig);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: PeerSetting
  ) => {
    setting.stateHandler(e.target.value, setter);
  };

  useEffect(() => {
    setter((prev) => {
      return {
        ...prev,
        sshConfig: showSSH ? sshConfig : undefined,
      };
    });
  }, [sshConfig, setter, showSSH]);

  return (
    <>
      {settings.map((setting, id) => {
        return (
          <RowWithTextField
            key={id}
            label={
              <Label>
                {setting.label}{' '}
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
                  defaultValue={setting.default}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleChange(e, setting)
                  }
                />
                {setting.tips && (
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                )}
              </div>
            }
          />
        );
      })}

      <Label
        as='label'
        style={{ marginTop: '1rem' }}
        variant='subheadline'
        colorName='lowContrast'
      >
        SSH Configuration
      </Label>
      <Label>
        You may provide SSH configuration to connect to your PostgreSQL database
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
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    sshParam.stateHandler(e.target.value, setSSHConfig)
                  }
                  type={sshParam.type}
                  defaultValue={
                    (sshConfig as SSHConfig)[
                      sshParam.label === 'BASE64 Private Key'
                        ? 'privateKey'
                        : (sshParam.label.toLowerCase() as
                            | 'host'
                            | 'port'
                            | 'user'
                            | 'password'
                            | 'privateKey')
                    ] || ''
                  }
                />
                {sshParam.tips && <InfoPopover tips={sshParam.tips} />}
              </div>
            }
          />
        ))}
    </>
  );
}
