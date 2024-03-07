'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { kaSetting } from '@/app/peers/create/[peerType]/helpers/ka';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton, RowWithTextField } from '@/lib/Layout';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import { InfoPopover } from '../InfoPopover';

interface KafkaProps {
  setter: PeerSetter;
}
const KafkaForm = ({ setter }: KafkaProps) => {
  return (
    <div>
      <Label>
      TODO TEXT
      </Label>
      {kaSetting.map((setting, index) => {
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
};

export default KafkaForm;
