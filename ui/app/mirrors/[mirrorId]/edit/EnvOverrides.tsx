'use client';

import { DBType, dBTypeFromJSON } from '@/grpc_generated/peers';
import {
  DynamicSetting,
  GetDynamicSettingsResponse,
} from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { useEffect, useMemo, useState } from 'react';
import { fieldStyle } from '../styles/edit.styles';

// Settings that are safe to override per mirror via CDCFlowConfigUpdate.UpdatedEnv,
// gated on the mirror's source type. The override is merged into the mirror's Env,
// which dynLookup checks before the service-level dynamic_settings catalog value.
const pipeLevelSettings: { name: string; sourceTypes: DBType[] }[] = [
  {
    name: 'PEERDB_MONGODB_EXCLUDED_OPERATION_TYPES',
    sourceTypes: [DBType.MONGO],
  },
];

type EnvOverridesProps = {
  sourceType?: DBType;
  currentEnv: { [key: string]: string };
  updatedEnv: { [key: string]: string };
  setUpdatedEnv: (env: { [key: string]: string }) => void;
};

export default function EnvOverrides({
  sourceType,
  currentEnv,
  updatedEnv,
  setUpdatedEnv,
}: EnvOverridesProps) {
  const applicableSettings = useMemo(() => {
    if (sourceType === undefined) {
      return [];
    }
    const normalizedSourceType = dBTypeFromJSON(sourceType);
    return pipeLevelSettings.filter((setting) =>
      setting.sourceTypes.includes(normalizedSourceType)
    );
  }, [sourceType]);
  const [serviceSettings, setServiceSettings] = useState<DynamicSetting[]>([]);

  useEffect(() => {
    if (applicableSettings.length === 0) {
      return;
    }
    fetch('/api/v1/dynamic_settings', { cache: 'no-store' })
      .then((res) => res.json())
      .then((res: GetDynamicSettingsResponse) =>
        setServiceSettings(res.settings ?? [])
      )
      .catch(() => setServiceSettings([]));
  }, [applicableSettings.length]);

  if (applicableSettings.length === 0) {
    return null;
  }

  const onSettingChange = (name: string, value: string) => {
    const newEnv = { ...updatedEnv };
    if (value === (currentEnv[name] ?? '')) {
      // unchanged from the mirror's current value: don't send it, so an untouched
      // field can't shadow the service-level default with an empty override
      delete newEnv[name];
    } else {
      newEnv[name] = value;
    }
    setUpdatedEnv(newEnv);
  };

  return (
    <>
      <Label variant='action' as='label' style={{ marginTop: '1rem' }}>
        Setting Overrides
      </Label>
      {applicableSettings.map(({ name }) => {
        const serviceSetting = serviceSettings.find(
          (setting) => setting.name === name
        );
        const serviceValue =
          serviceSetting?.value || serviceSetting?.defaultValue;
        return (
          <RowWithTextField
            key={name}
            label={
              <Label>
                {name}
                {serviceSetting && (
                  <Label
                    as='label'
                    style={{ fontSize: 13, display: 'block', padding: 0 }}
                  >
                    {serviceSetting.description}
                  </Label>
                )}
              </Label>
            }
            action={
              <div style={fieldStyle}>
                <TextField
                  variant='simple'
                  placeholder={
                    serviceValue ? `${serviceValue} (service default)` : ''
                  }
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    onSettingChange(name, e.target.value)
                  }
                  value={updatedEnv[name] ?? currentEnv[name] ?? ''}
                />
              </div>
            }
          />
        );
      })}
    </>
  );
}
