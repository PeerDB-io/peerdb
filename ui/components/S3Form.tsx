'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import {
  blankPostgresSetting,
  postgresSetting,
} from '@/app/peers/create/[peerType]/helpers/pg';
import { s3Setting } from '@/app/peers/create/[peerType]/helpers/s3';
import { PostgresConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import { PeerSetter } from './ConfigForm';
import { InfoPopover } from './InfoPopover';

interface S3Props {
  setter: PeerSetter;
}
const S3ConfigForm = ({ setter }: S3Props) => {
  const [metadataDB, setMetadataDB] =
    useState<PeerConfig>(blankPostgresSetting);
  useEffect(() => {
    setter((prev) => {
      return {
        ...prev,
        metadataDb: metadataDB as PostgresConfig,
      };
    });
  }, [metadataDB]);
  return (
    <>
      {s3Setting.map((setting, index) => {
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
        Metadata Database
      </Label>

      {postgresSetting.map((setting, index) => {
        <RowWithTextField
          key={index}
          label={
            <Label>
              {setting.label}{' '}
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
                  setting.stateHandler(e.target.value, setMetadataDB)
                }
              />
              {setting.tips && (
                <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
              )}
            </div>
          }
        />;
      })}
    </>
  );
};

export default S3ConfigForm;
