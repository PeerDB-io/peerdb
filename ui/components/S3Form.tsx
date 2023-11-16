'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import { postgresSetting } from '@/app/peers/create/[peerType]/helpers/pg';
import {
  blankS3Setting,
  s3Setting,
} from '@/app/peers/create/[peerType]/helpers/s3';
import { PostgresConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton, RowWithTextField } from '@/lib/Layout';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import { PeerSetter } from './ConfigForm';
import { InfoPopover } from './InfoPopover';

interface S3Props {
  setter: PeerSetter;
}
const S3ConfigForm = ({ setter }: S3Props) => {
  const [metadataDB, setMetadataDB] = useState<PeerConfig>(
    blankS3Setting.metadataDb!
  );
  const [storageType, setStorageType] = useState<'S3' | 'GCS'>('S3');
  const displayCondition = (label: string) => {
    return !(
      (label === 'Region' || label === 'Role ARN') &&
      storageType === 'GCS'
    );
  };
  useEffect(() => {
    const endpoint = storageType === 'S3' ? '' : 'storage.googleapis.com';
    setter((prev) => {
      return {
        ...prev,
        metadataDb: metadataDB as PostgresConfig,
        endpoint,
      };
    });

    if (storageType === 'GCS') {
      setter((prev) => {
        return {
          ...prev,
          region: 'auto',
        };
      });
    }
  }, [metadataDB, storageType, setter]);

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
          href='https://docs.peerdb.io/sql/commands/create-peer#storage-peers-s3-and-gcs'
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

      <Label
        as='label'
        style={{ marginTop: '1rem' }}
        variant='subheadline'
        colorName='lowContrast'
      >
        Metadata Database
      </Label>
      <Label>
        For S3/GCS storage peers, PeerDB uses an external PostgreSQL database to
        store metadata (last sync state) for mirrors.
        <br></br>
        More information on creation of storage peers in PeerDB{' '}
        <a
          style={{ color: 'teal' }}
          href='https://docs.peerdb.io/sql/commands/create-peer#storage-peers-s3-and-gcs'
        >
          here.
        </a>
      </Label>
      {postgresSetting.map(
        (pgSetting, index) =>
          pgSetting.label !== 'Transaction Snapshot' && (
            <RowWithTextField
              key={index}
              label={
                <Label>
                  {pgSetting.label}{' '}
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
                      pgSetting.stateHandler(e.target.value, setMetadataDB)
                    }
                    defaultValue={
                      (metadataDB as PostgresConfig)[
                        pgSetting.label.toLowerCase() as keyof PostgresConfig
                      ] || ''
                    }
                  />
                  {pgSetting.tips && (
                    <InfoPopover
                      tips={pgSetting.tips}
                      link={pgSetting.helpfulLink}
                    />
                  )}
                </div>
              }
            />
          )
      )}
    </div>
  );
};

export default S3ConfigForm;
