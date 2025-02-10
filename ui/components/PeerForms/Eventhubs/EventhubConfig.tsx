'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { ehSetting } from '@/app/peers/create/[peerType]/helpers/eh';
import { notifyErr } from '@/app/utils/notify';
import InfoPopover from '@/components/InfoPopover';
import { EventHubConfig } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { Dispatch, SetStateAction, useState } from 'react';
import { ehSchema } from './schema';

interface EventhubConfigProps {
  eventhubConfig: EventHubConfig & {
    id: number;
  };
  updateForms: Dispatch<
    SetStateAction<
      (EventHubConfig & {
        id: number;
      })[]
    >
  >;
  onDelete: () => void;
}

export default function EventhubsConfig({
  eventhubConfig,
  updateForms,
  onDelete,
}: EventhubConfigProps) {
  const [ehConfig, setEhConfig] = useState<
    EventHubConfig & {
      id: number;
    }
  >(eventhubConfig);
  const [collapsed, setCollapsed] = useState<boolean>(false);

  const handleSave = () => {
    const ehConfigValidity = ehSchema.safeParse(ehConfig);
    if (ehConfigValidity.success) {
      updateForms((prev) =>
        prev.map((config) => {
          if (config.id === ehConfig.id) {
            return ehConfig;
          }
          return config;
        })
      );
      setCollapsed(true);
    } else {
      notifyErr(ehConfigValidity.error.issues[0].message);
    }
  };

  const getFieldValue = (label: string) => {
    if (label === 'Namespace') {
      return ehConfig.namespace;
    }
    if (label === 'Subscription ID') {
      return ehConfig.subscriptionId;
    }
    if (label === 'Resource Group') {
      return ehConfig.resourceGroup;
    }
    if (label === 'Location') {
      return ehConfig.location;
    }
    if (label === 'Partition Count') {
      return ehConfig.partitionCount;
    }
    if (label === 'Message Retention (Days)') {
      return ehConfig.messageRetentionInDays;
    }
    return '';
  };

  return (
    <div
      style={{
        padding: !collapsed ? '1rem' : '0.5rem',
        border: '1px solid rgba(0,0,0,0.1)',
        borderRadius: '1rem',
        display: collapsed ? 'flex' : 'block',
        justifyContent: collapsed ? 'space-between' : 'auto',
        width: 'fit-content',
      }}
    >
      {collapsed ? (
        <Label as='label' style={{ fontSize: 14 }}>
          {ehConfig.namespace || 'Yet to be configured'}
        </Label>
      ) : (
        ehSetting.map((setting, index) => {
          return (
            <RowWithTextField
              key={index}
              label={
                <Label as='label' style={{ fontSize: 14 }}>
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
                    value={getFieldValue(setting.label)}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      setting.stateHandler(
                        e.target.value,
                        setEhConfig as PeerSetter
                      )
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
        })
      )}
      <div
        style={{
          display: 'flex',
          justifyContent: collapsed ? 'flex-end' : 'space-between',
          marginTop: !collapsed ? '1rem' : 'auto',
        }}
      >
        {!collapsed && (
          <Button variant='normalSolid' onClick={handleSave}>
            Save
          </Button>
        )}
        <div
          style={{
            display: 'flex',
            flexDirection: 'row',
            alignItems: 'center',
            columnGap: '0.5rem',
          }}
        >
          <Button
            variant='normal'
            aria-label='icon-button'
            onClick={() => setCollapsed((prev) => !prev)}
          >
            <Icon name={collapsed ? 'arrow_drop_down' : 'arrow_drop_up'} />
          </Button>
          <Button variant='drop' aria-label='icon-button' onClick={onDelete}>
            <Icon name='delete' />
          </Button>
        </div>
      </div>
    </div>
  );
}
