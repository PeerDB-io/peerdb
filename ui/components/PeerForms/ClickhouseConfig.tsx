'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import { Button } from '@/lib/Button/Button';
import { Icon } from '@/lib/Icon/Icon';
import { Label } from '@/lib/Label';
import { RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useState } from 'react';
import { InfoPopover } from '../InfoPopover';
import Link from 'next/link';
interface ConfigProps {
  settings: PeerSetting[];
  setter: PeerSetter;
}

export default function ClickhouseForm({ settings, setter }: ConfigProps) {
  const [show, setShow] = useState(false);
  const S3Labels = [
    'S3 Path',
    'Access Key ID',
    'Secret Access Key',
    'Region',
    'Endpoint',
  ];
  const handleChange = (val: string | boolean, setting: PeerSetting) => {
    setting.stateHandler(val, setter);
  };

  return (
    <>
      {settings
        .filter((setting) => !S3Labels.includes(setting.label))
        .map((setting, id) => {
          return setting.type == 'switch' ? (
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
                    type={setting.type}
                    defaultValue={setting.default}
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
          );
        })}

      <Label variant='subheadline' as='label' style={{ marginTop: '2rem' }}>
        Transient S3 Stage (Optional)
      </Label>
      <Label as='label' style={{ display: 'block' }}>
        PeerDB loads rows as files in an internal staging environment during
        CDC, under the covers.
        <br></br>
        If you want this stage to belong to you, you can configure a bucket
        below.
        <br></br>
        <Link style={{color:'teal', cursor:'pointer'}} target='_blank' href='https://docs.peerdb.io/connect/s3'>
          Setting up an S3 bucket
        </Link>
      </Label>
      <Button
        className='IconButton'
        aria-label='collapse'
        onClick={() => {
          setShow((prev) => !prev);
        }}
        style={{
          height: '2em',
          marginTop: '0.5rem',
          marginBottom: '1rem',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center' }}>
          <h3 style={{ marginRight: '10px', fontSize: 15 }}>
            Configure a stage
          </h3>
          <Icon name={`keyboard_double_arrow_${show ? 'up' : 'down'}`} />
        </div>
      </Button>

      {show &&
        settings
          .filter((setting) => S3Labels.includes(setting.label))
          .map((setting, id) => (
            <RowWithTextField
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
                  <TextField
                    variant='simple'
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      handleChange(e.target.value, setting)
                    }
                    type={setting.type}
                    placeholder={setting.placeholder}
                  />
                  {setting.tips && <InfoPopover tips={setting.tips} />}
                </div>
              }
            />
          ))}
    </>
  );
}
