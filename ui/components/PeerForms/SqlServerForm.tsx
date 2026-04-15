'use client';

import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import InfoPopover from '@/components/InfoPopover';
import { SqlServerConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { handleFieldChange } from './common';

interface SqlServerProps {
  settings: PeerSetting[];
  setter: PeerSetter;
  config: SqlServerConfig;
}

export default function SqlServerForm({ settings, setter }: SqlServerProps) {
  return (
    <>
      {settings.map((setting, id) => (
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
                type={setting.type}
                defaultValue={setting.default}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  handleFieldChange(e, setting, setter)
                }
              />
              {setting.tips && (
                <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
              )}
            </div>
          }
        />
      ))}
    </>
  );
}
