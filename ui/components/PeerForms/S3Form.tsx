'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { s3Setting } from '@/app/peers/create/[peerType]/helpers/s3';
import { GCS_ENDPOINT } from '@/app/utils/gcsEndpoint';
import InfoPopover from '@/components/InfoPopover';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton, RowWithTextField } from '@/lib/Layout';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';

interface S3Props {
  setter: PeerSetter;
}

export default function S3Form({ setter }: S3Props) {
  const [storageType, setStorageType] = useState<'S3' | 'GCS'>('S3');
  const displayCondition = (label: string) => {
    return !(
      (label === 'Region' || label === 'Role ARN' || label === 'Endpoint') &&
      storageType === 'GCS'
    );
  };

  useEffect(() => {
    if (storageType === 'GCS') {
      setter((prev) => {
        return {
          ...prev,
          region: 'auto',
          endpoint: GCS_ENDPOINT,
        };
      });
    }
  }, [storageType, setter]);

  return (
    <div>
      <Label>
        PeerDB supports S3 and GCS storage peers.
        <br></br>
        In case of GCS, your access key ID and secret access key will be your
        HMAC key and HMAC secret respectively.
        <br></br>
        <a
          style={{ color: 'teal' }}
          href='https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create'
        >
          More information on how to setup HMAC for GCS.
        </a>
      </Label>
      <RadioButtonGroup
        style={{ display: 'flex' }}
        defaultValue={storageType}
        onValueChange={(val) => setStorageType(val as 'S3' | 'GCS')}
      >
        <RowWithRadiobutton
          label={<Label>S3</Label>}
          action={<RadioButton value='S3' />}
        />
        <RowWithRadiobutton
          label={<Label>GCS</Label>}
          action={<RadioButton value='GCS' />}
        />
      </RadioButtonGroup>
      {s3Setting.map((setting, index) => {
        if (displayCondition(setting.label))
          return (
            <RowWithTextField
              key={index}
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
          );
      })}
    </div>
  );
}
