'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { kaSetting } from '@/app/peers/create/[peerType]/helpers/ka';
import SelectTheme from '@/app/styles/select';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import ReactSelect from 'react-select';
import { InfoPopover } from '../InfoPopover';
interface KafkaProps {
  setter: PeerSetter;
}

const saslOptions = [
  { value: 'PLAIN', label: 'PLAIN' },
  { value: 'SCRAM-SHA-256', label: 'SCRAM-SHA-256' },
  { value: 'SCRAM-SHA-512', label: 'SCRAM-SHA-512' },
];

const KafkaForm = ({ setter }: KafkaProps) => {
  return (
    <div>
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
            label={<Label>SASL Mechanism</Label>}
            action={
              <ReactSelect
                key={index}
                placeholder='Select a mechanism'
                onChange={(val) =>
                  val && setting.stateHandler(val.value, setter)
                }
                options={saslOptions}
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
};

export default KafkaForm;
