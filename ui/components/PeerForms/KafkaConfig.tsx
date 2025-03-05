'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { kaSetting } from '@/app/peers/create/[peerType]/helpers/ka';
import SelectTheme from '@/app/styles/select';
import InfoPopover from '@/components/InfoPopover';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import ReactSelect from 'react-select';

interface KafkaProps {
  setter: PeerSetter;
}

export default function KafkaForm({ setter }: KafkaProps) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', rowGap: '0.5rem' }}>
      {kaSetting.map((setting, index) => {
        return setting.type === 'switch' ? (
          <RowWithSwitch
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
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <Switch
                  onCheckedChange={(state: boolean) =>
                    setting.stateHandler(state, setter)
                  }
                />
                {setting.tips && (
                  <InfoPopover tips={setting.tips} link={setting.helpfulLink} />
                )}
              </div>
            }
          />
        ) : setting.type === 'select' ? (
          <RowWithSelect
            key={index}
            label={<Label>{setting.label}</Label>}
            action={
              <ReactSelect
                placeholder={setting.placeholder}
                onChange={(val) =>
                  val && setting.stateHandler(val.value, setter)
                }
                options={setting.options}
                theme={SelectTheme}
              />
            }
          />
        ) : (
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
    </div>
  );
}
