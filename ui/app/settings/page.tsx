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
import { Table, TableCell, TableRow } from '@/lib/Table';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { MaterialSymbol } from 'material-symbols';
import { useEffect, useMemo, useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { notifyErr } from '../utils/notify';

const ROWS_PER_PAGE = 7;

const ApplyModeIconWithTooltip = ({ applyMode }: { applyMode: number }) => {
  let tooltipText = '';
  let iconName: MaterialSymbol = 'help';

  switch (applyMode) {
    case DynconfApplyMode.APPLY_MODE_IMMEDIATE:
      tooltipText = 'Changes to this configuration will apply immediately';
      iconName = 'bolt';
      break;
    case DynconfApplyMode.APPLY_MODE_AFTER_RESUME:
      tooltipText = 'Changes to this configuration will apply after resume';
      iconName = 'cached';
      break;
    case DynconfApplyMode.APPLY_MODE_RESTART:
      tooltipText =
        'Changes to this configuration will apply after server restart.';
      iconName = 'restart_alt';
      break;
    case DynconfApplyMode.APPLY_MODE_NEW_MIRROR:
      tooltipText =
        'Changes to this configuration will apply only to new mirrors';
      iconName = 'new_window';
      break;
    default:
      tooltipText = 'Unknown apply mode';
      iconName = 'help';
  }

  return (
    <div style={{ cursor: 'help' }}>
      <Tooltip style={{ width: '100%' }} content={tooltipText}>
        <Icon name={iconName} />
      </Tooltip>
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

  const handleEdit = () => {
    setEditMode(true);
  };

  const validateNewValue = (): boolean => {
    const notNullValue = newValue ?? '';
    if (setting.valueType === DynconfValueType.INT) {
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
    } else if (setting.valueType === DynconfValueType.UINT) {
      const a = parseInt(Number(notNullValue).toString());
      if (isNaN(a) || a > Number.MAX_SAFE_INTEGER || a < 0) {
        notifyErr(
          'Invalid value. Please enter a valid 64-bit unsigned integer.'
        );
        return false;
      }
      return true;
    } else if (setting.valueType === DynconfValueType.BOOL) {
      if (notNullValue !== 'true' && notNullValue !== 'false') {
        notifyErr('Invalid value. Please enter true or false.');
        return false;
      }
      return true;
    } else if (setting.valueType === DynconfValueType.STRING) {
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
    await fetch('/api/settings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(updatedSetting),
    });
    setEditMode(false);
    onSettingUpdate();
  };

  return (
    <TableRow key={setting.name}>
      <TableCell style={{ width: '15%' }}>
        <Label>{setting.name}</Label>
      </TableCell>
      <TableCell style={{ width: '10%' }}>
        {editMode ? (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <TextField
              value={newValue ?? ''}
              onChange={(e) => setNewValue(e.target.value)}
              variant='simple'
            />
            <Button variant='normalBorderless' onClick={handleSave}>
              <Icon name='save' />
            </Button>
          </div>
        ) : (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            {setting.value || 'N/A'}
            <Button variant='normalBorderless' onClick={handleEdit}>
              <Icon name='edit' />
            </Button>
          </div>
        )}
      </TableCell>
      <TableCell style={{ width: '20%' }}>
        {setting.defaultValue || 'N/A'}
      </TableCell>
      <TableCell style={{ width: '45%' }}>
        {setting.description || 'N/A'}
      </TableCell>
      <TableCell style={{ width: '10%' }}>
        <ApplyModeIconWithTooltip applyMode={setting.applyMode || 0} />
      </TableCell>
    </TableRow>
  );
};

const SettingsPage = () => {
  const [settings, setSettings] = useState<GetDynamicSettingsResponse>({
    settings: [],
  });
  const [currentPage, setCurrentPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('asc');
  const sortField = 'name';

  const fetchSettings = async () => {
    const response = await fetch('/api/settings');
    const data = await response.json();
    setSettings(data);
  };

  useEffect(() => {
    fetchSettings();
  }, []);

  const filteredSettings = useMemo(
    () =>
      settings.settings
        .filter((setting) =>
          setting.name.toLowerCase().includes(searchQuery.toLowerCase())
        )
        .sort((a, b) => {
          const aValue = a[sortField];
          const bValue = b[sortField];
          if (aValue < bValue) return sortDir === 'dsc' ? 1 : -1;
          if (aValue > bValue) return sortDir === 'dsc' ? -1 : 1;
          return 0;
        }),
    [settings, searchQuery, sortDir]
  );
  const totalPages = Math.ceil(filteredSettings.length / ROWS_PER_PAGE);
  const displayedSettings = useMemo(() => {
    const startRow = (currentPage - 1) * ROWS_PER_PAGE;
    const endRow = startRow + ROWS_PER_PAGE;
    return filteredSettings.slice(startRow, endRow);
  }, [filteredSettings, currentPage]);

  const handlePrevPage = () => {
    if (currentPage > 1) setCurrentPage(currentPage - 1);
  };

  const handleNextPage = () => {
    if (currentPage < totalPages) setCurrentPage(currentPage + 1);
  };

  return (
    <div>
      <Table
        title={<Label variant='headline'>Settings List</Label>}
        toolbar={{
          left: (
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <Button variant='normalBorderless' onClick={handlePrevPage}>
                <Icon name='chevron_left' />
              </Button>
              <Button variant='normalBorderless' onClick={handleNextPage}>
                <Icon name='chevron_right' />
              </Button>
              <Label>{`${currentPage} of ${totalPages}`}</Label>
              <Button variant='normalBorderless' onClick={fetchSettings}>
                <Icon name='refresh' />
              </Button>
              <button
                className='IconButton'
                onClick={() => setSortDir('asc')}
                aria-label='sort up'
                style={{ color: sortDir == 'asc' ? 'green' : 'gray' }}
              >
                <Icon name='arrow_upward' />
              </button>
              <button
                className='IconButton'
                onClick={() => setSortDir('dsc')}
                aria-label='sort down'
                style={{ color: sortDir == 'dsc' ? 'green' : 'gray' }}
              >
                <Icon name='arrow_downward' />
              </button>
            </div>
          ),
          right: (
            <SearchField
              placeholder='Search by config name'
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          ),
        }}
        header={
          <TableRow>
            {[
              { header: 'Configuration Name', width: '35%' },
              { header: 'Current Value', width: '10%' },
              { header: 'Default Value', width: '10%' },
              { header: 'Description', width: '35%' },
              { header: 'Apply Mode', width: '10%' },
            ].map(({ header, width }) => (
              <TableCell key={header} as='th' style={{ width }}>
                {header}
              </TableCell>
            ))}
          </TableRow>
        }
      >
        {displayedSettings.map((setting) => (
          <DynamicSettingItem
            key={setting.name}
            setting={setting}
            onSettingUpdate={fetchSettings}
          />
        ))}
      </Table>
      <ToastContainer />
    </div>
  );
};

export default SettingsPage;
