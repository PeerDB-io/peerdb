'use client';

import { DynconfApplyMode } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { dynamic_settings } from '@prisma/client';
import { MaterialSymbol } from 'material-symbols';
import { useEffect, useMemo, useState } from 'react';

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
  setting: dynamic_settings;
  onSettingUpdate: () => void;
}) => {
  const [editMode, setEditMode] = useState(false);
  const [newValue, setNewValue] = useState(setting.config_value);

  const handleEdit = () => {
    setEditMode(true);
  };

  const handleSave = async () => {
    const updatedSetting = { ...setting, config_value: newValue };
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
    <TableRow key={setting.id}>
      <TableCell style={{ width: '35%' }}>
        <Label>{setting.config_name}</Label>
      </TableCell>
      <TableCell style={{ width: '10%' }}>
        {editMode ? (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <TextField
              value={newValue || undefined}
              onChange={(e) => setNewValue(e.target.value)}
              variant='simple'
            />
            <Button variant='normalBorderless' onClick={handleSave}>
              <Icon name='save' />
            </Button>
          </div>
        ) : (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            {setting.config_value || 'N/A'}
            <Button variant='normalBorderless' onClick={handleEdit}>
              <Icon name='edit' />
            </Button>
          </div>
        )}
      </TableCell>
      <TableCell style={{ width: '10%' }}>
        {setting.config_default_value || 'N/A'}
      </TableCell>
      <TableCell style={{ width: '35%' }}>
        {setting.config_description || 'N/A'}
      </TableCell>
      <TableCell style={{ width: '10%' }}>
        <ApplyModeIconWithTooltip applyMode={setting.config_apply_mode || 0} />
      </TableCell>
    </TableRow>
  );
};

const SettingsPage = () => {
  const [settings, setSettings] = useState<dynamic_settings[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortDir, setSortDir] = useState<'asc' | 'dsc'>('asc');
  const sortField = 'config_name';

  const fetchSettings = async () => {
    const response = await fetch('/api/settings');
    const data = await response.json();
    setSettings(data);
  };

  useEffect(() => {
    fetchSettings();
  }, []);

  const totalPages = Math.ceil(settings.length / ROWS_PER_PAGE);

  const displayedSettings = useMemo(() => {
    const filteredSettings = settings.filter((setting) =>
      setting.config_name.toLowerCase().includes(searchQuery.toLowerCase())
    );
    filteredSettings.sort((a, b) => {
      const aValue = a[sortField];
      const bValue = b[sortField];
      if (aValue === null || bValue === null) return 0;
      if (aValue < bValue) return sortDir === 'dsc' ? 1 : -1;
      if (aValue > bValue) return sortDir === 'dsc' ? -1 : 1;
      return 0;
    });

    const startRow = (currentPage - 1) * ROWS_PER_PAGE;
    const endRow = startRow + ROWS_PER_PAGE;
    return filteredSettings.slice(startRow, endRow);
  }, [settings, currentPage, searchQuery, sortField, sortDir]);

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
            key={setting.id}
            setting={setting}
            onSettingUpdate={fetchSettings}
          />
        ))}
      </Table>
    </div>
  );
};

export default SettingsPage;
