import { PeerSetter } from '@/app/dto/PeersDTO';
import { mongoSetting } from '@/app/peers/create/[peerType]/helpers/mo';
import SelectTheme from '@/app/styles/select';
import InfoPopover from '@/components/InfoPopover';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import ReactSelect from 'react-select';

interface MongoFormProps {
  setter: PeerSetter;
}

export default function MongoForm({ setter }: MongoFormProps) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', rowGap: '0.5rem' }}>
      {mongoSetting.map((setting, index) => {
        return setting.type === 'select' ? (
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
                  style={{
                    border: 'auto',
                    width: '100%',
                  }}
                  type={setting.type || 'text'}
                  defaultValue={setting.default}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    setting.stateHandler(e.target.value, setter)
                  }
                  placeholder={setting.placeholder}
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