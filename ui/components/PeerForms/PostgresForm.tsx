'use client';
import { PeerConfig, PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import {
  SSHSetting,
  blankSSHConfig,
  sshSetting,
} from '@/app/peers/create/[peerType]/helpers/ssh';
import InfoPopover from '@/components/InfoPopover';
import { SSHConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useSearchParams } from 'next/navigation';
import { useEffect, useState } from 'react';
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
  const [showSSH, setShowSSH] = useState(false);
  const [sshConfig, setSSHConfig] = useState(blankSSHConfig);

  const handleCa = (
    file: File,
    setFile: (value: string, setter: PeerSetter) => void
  ) => {
    if (file) {
      const reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        setFile(reader.result as string, setter);
      };
      reader.onerror = (error) => {
        console.log(error);
      };
    }
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: PeerSetting
  ) => {
    if (setting.type === 'file') {
      if (e.target.files) handleCa(e.target.files[0], setting.stateHandler);
    } else {
      setting.stateHandler(e.target.value, setter);
    }
  };

  const handleSSHParam = (
    e: React.ChangeEvent<HTMLInputElement>,
    setting: SSHSetting
  ) => {
    if (setting.type === 'file') {
      if (e.target.files) {
        const file = e.target.files[0];
        if (file) {
          const reader = new FileReader();
          reader.readAsText(file);
          reader.onload = () => {
            const fileContents = reader.result as string;
            const base64EncodedContents = Buffer.from(
              fileContents,
              'utf-8'
            ).toString('base64');
            setting.stateHandler(base64EncodedContents, setSSHConfig);
          };
          reader.onerror = (error) => {
            console.log(error);
          };
        }
      }
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
        return setting.type === 'switch' ? (
          <RowWithSwitch
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
        ) : (
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
                  style={
                    setting.type === 'file'
                      ? { border: 'none', height: 'auto' }
                      : { border: 'auto' }
                  }
                  type={setting.type}
                  defaultValue={setting.default}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleTextFieldChange(e, setting)
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
