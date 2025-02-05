import { PeerSetter } from '@/app/dto/PeersDTO';
import { esSetting } from '@/app/peers/create/[peerType]/helpers/es';
import SelectTheme from '@/app/styles/select';
import InfoPopover from '@/components/InfoPopover';
import {
  ElasticsearchAuthType,
  ElasticsearchConfig,
} from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import ReactSelect from 'react-select';

interface ElasticsearchProps {
  config: ElasticsearchConfig;
  setter: PeerSetter;
}

export default function ElasticsearchConfigForm({
  config,
  setter,
}: ElasticsearchProps) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', rowGap: '0.5rem' }}>
      {esSetting.map((setting, index) => {
        return setting.type === 'select' ? (
          <RowWithSelect
            label={<Label>{setting.label}</Label>}
            action={
              <ReactSelect
                key={index}
                placeholder={setting.placeholder}
                onChange={(val) =>
                  val && setting.stateHandler(val.value, setter)
                }
                options={setting.options}
                theme={SelectTheme}
              />
            }
          />
        ) : (setting.label === 'API Key' &&
            config.authType === ElasticsearchAuthType.APIKEY) ||
          (setting.label !== 'API Key' &&
            config.authType === ElasticsearchAuthType.BASIC) ||
          setting.label === 'Addresses' ? (
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
        ) : (
          <></>
        );
      })}
    </div>
  );
}
