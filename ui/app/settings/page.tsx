'use client';

import { DynconfApplyMode, DynconfValueType } from '@/grpc_generated/flow';
import {
  DynamicSetting,
  GetDynamicSettingsResponse,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { TextField } from '@/lib/TextField';
import { useEffect, useMemo, useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { notifyErr } from '../utils/notify';

const ROWS_PER_PAGE = 7;

const ApplyModeIconWithTooltip = ({ applyMode }: { applyMode: number }) => {
  let tooltipText = '';

  switch (applyMode.toString()) {
    case DynconfApplyMode[DynconfApplyMode.APPLY_MODE_IMMEDIATE].toString():
      tooltipText = 'Changes to this configuration will apply immediately';
      break;
    case DynconfApplyMode[DynconfApplyMode.APPLY_MODE_AFTER_RESUME].toString():
      tooltipText = 'Changes to this configuration will apply after resume';
      break;
    case DynconfApplyMode[DynconfApplyMode.APPLY_MODE_RESTART].toString():
      tooltipText =
        'Changes to this configuration will apply after server restart.';
      break;
    case DynconfApplyMode[DynconfApplyMode.APPLY_MODE_NEW_MIRROR].toString():
      tooltipText =
        'Changes to this configuration will apply only to new mirrors';
      break;
    default:
      tooltipText = 'Unknown apply mode';
  }

  return (
    <div style={{ cursor: 'help' }}>
      <Label style={{ fontSize: 14, padding: 0 }}>{tooltipText}</Label>
    </div>
  );
};
const DynamicSettingItem = ({
  setting,
  onSettingUpdate,
}: {
  setting: DynamicSetting;
  onSettingUpdate: () => void;
}) => {
  const [editMode, setEditMode] = useState(false);
  const [newValue, setNewValue] = useState(setting.value);
  const [showDescription, setShowDescription] = useState(false);
  const handleEdit = () => {
    setEditMode(true);
  };

  const validateNewValue = (): boolean => {
    const notNullValue = newValue ?? '';
    if (
      setting.valueType.toString() === DynconfValueType[DynconfValueType.INT]
    ) {
      const a = parseInt(Number(notNullValue).toString());
      if (
        isNaN(a) ||
        a > Number.MAX_SAFE_INTEGER ||
        a < Number.MIN_SAFE_INTEGER
      ) {
        notifyErr('Invalid value. Please enter a valid 64-bit signed integer.');
        return false;
      }
      return true;
    } else if (
      setting.valueType.toString() === DynconfValueType[DynconfValueType.UINT]
    ) {
      const a = parseInt(Number(notNullValue).toString());
      if (isNaN(a) || a > Number.MAX_SAFE_INTEGER || a < 0) {
        notifyErr(
          'Invalid value. Please enter a valid 64-bit unsigned integer.'
        );
        return false;
      }
      return true;
    } else if (
      setting.valueType.toString() === DynconfValueType[DynconfValueType.BOOL]
    ) {
      if (notNullValue !== 'true' && notNullValue !== 'false') {
        notifyErr('Invalid value. Please enter true or false.');
        return false;
      }
      return true;
    } else if (
      setting.valueType.toString() === DynconfValueType[DynconfValueType.STRING]
    ) {
      return true;
    } else {
      notifyErr('Invalid value type');
      return false;
    }
  };

  const handleSave = async () => {
    if (!validateNewValue() || newValue === setting.value) {
      setNewValue(setting.value);
      setEditMode(false);
      return;
    }
    const updatedSetting = { ...setting, value: newValue };
    await fetch('/api/v1/dynamic_settings', {
      method: 'POST',
      body: JSON.stringify(updatedSetting),
    });
    setEditMode(false);
    onSettingUpdate();
  };

  return (
    <div
      style={{
        borderRadius: '0.5rem',
        padding: '0.5rem',
        border: '1px solid rgba(0, 0, 0, 0.07)',
        display: 'flex',
        flexDirection: 'column',
        height: 'fit-content',
      }}
    >
      <div>
        <Label as='label' style={{ padding: 0, fontSize: 14, fontWeight: 500 }}>
          {setting.name}
        </Label>
      </div>
      <div>
        <div>
          <div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                marginTop: '0.5rem',
                marginBottom: '0.5rem',
              }}
            >
              <TextField
                style={{ fontSize: 14 }}
                value={editMode ? (newValue ?? '') : setting.value}
                placeholder='N/A'
                onChange={(e) => setNewValue(e.target.value)}
                variant='simple'
                readOnly={!editMode}
                disabled={!editMode}
              />
              <Button
                variant='normalBorderless'
                onClick={editMode ? handleSave : handleEdit}
              >
                <Icon name={!editMode ? 'edit' : 'save'} />
              </Button>
            </div>
          </div>

          <div>
            <Label style={{ padding: 0, fontSize: 14 }}>
              {' '}
              Default:<b> {setting.defaultValue || 'N/A'} </b>
            </Label>
          </div>
          <div>
            <ApplyModeIconWithTooltip applyMode={setting.applyMode || 0} />
          </div>
          <Button
            style={{ marginTop: '0.5rem' }}
            onClick={() => setShowDescription((prev) => !prev)}
          >
            <Label style={{ padding: 0, fontSize: 13 }}> More info </Label>
            <Icon
              name={
                !showDescription ? 'arrow_downward_alt' : 'arrow_upward_alt'
              }
            />
          </Button>
          {showDescription && (
            <div>
              <Label style={{ padding: 0, fontSize: 14 }}>
                {setting.description || 'N/A'}
              </Label>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const SettingsPage = () => {
  const [settings, setSettings] = useState<GetDynamicSettingsResponse>({
    settings: [],
  });
  const [searchQuery, setSearchQuery] = useState('');

  const fetchSettings = async () => {
    const response = await fetch('/api/v1/dynamic_settings');
    const data = await response.json();
    setSettings(data);
  };

  useEffect(() => {
    fetchSettings();
  }, []);

  const filteredSettings = useMemo(
    () =>
      settings.settings.filter((setting) =>
        setting.name.toLowerCase().includes(searchQuery.toLowerCase())
      ),
    [settings, searchQuery]
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        padding: '1rem',
        rowGap: '1rem',
      }}
    >
      <Label variant='title3'>Settings</Label>
      <SearchField
        placeholder='Search by config name'
        onChange={(e) => setSearchQuery(e.target.value)}
        style={{ fontSize: 13 }}
      />
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
          gap: '16px',
        }}
      >
        {filteredSettings.map((setting) => (
          <DynamicSettingItem
            key={setting.name}
            setting={setting}
            onSettingUpdate={fetchSettings}
          />
        ))}
        <ToastContainer />
      </div>
    </div>
  );
};

export default SettingsPage;
