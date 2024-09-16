'use client';
import { PeerConfig, PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import {
  SSHSetting,
  blankSSHConfig,
  sshSetter,
  sshSetting,
} from '@/app/peers/create/[peerType]/helpers/pg';
import { SSHConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useSearchParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import { InfoPopover } from '../InfoPopover';
interface ConfigProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: PeerConfig;
  type: string;
}

export default function PostgresForm({
  settings,
  config,
  setter,
  type,
}: ConfigProps) {
  const searchParams = useSearchParams();
  const [showSSH, setShowSSH] = useState<boolean>(false);
  const [sshConfig, setSSHConfig] = useState<SSHConfig>(blankSSHConfig);
  const handleFile = (
    file: File,
    setFile: (value: string, configSetter: sshSetter) => void
  ) => {
    if (file) {
      const reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        const fileContents = reader.result as string;
        const base64EncodedContents = Buffer.from(
          fileContents,
          'utf-8'
        ).toString('base64');
        setFile(base64EncodedContents, setSSHConfig);
      };
      reader.onerror = (error) => {
        console.log(error);
      };
    }
  };

  const handleSSHParam = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: SSHSetting
  ) => {
    if (setting.type === 'file') {
      if (e.target.files) handleFile(e.target.files[0], setting.stateHandler);
    } else {
      setting.stateHandler(e.target.value, setSSHConfig);
    }
  };

  useEffect(() => {
    const host = searchParams.get('host');
    if (host) setter((curr) => ({ ...curr, host }));
    const database = searchParams.get('db');
    if (database) setter((curr) => ({ ...curr, database }));
  }, [setter, searchParams]);

  useEffect(() => {
    setter((prev) => ({
      ...prev,
      sshConfig: showSSH ? sshConfig : undefined,
    }));
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
                  value={
                    setting.field && config[setting.field as keyof PeerConfig]
                  }
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    setting.stateHandler(e.target.value, setter)
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
        style={{ marginTop: '1rem', display: 'block' }}
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
                  variant={'simple'}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleSSHParam(e, sshParam)
                  }
                  style={{
                    border: sshParam.type === 'file' ? 'none' : 'auto',
                    height: sshParam.type === 'textarea' ? '15rem' : 'auto',
                  }}
                  type={sshParam.type}
                  defaultValue={
                    (sshConfig as SSHConfig)[
                      sshParam.label === 'SSH Private Key'
                        ? 'privateKey'
                        : sshParam.label === "Host's Public Key"
                          ? 'hostKey'
                          : (sshParam.label.toLowerCase() as keyof SSHConfig)
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
