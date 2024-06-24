'use client';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Dispatch, SetStateAction, useEffect, useMemo, useState } from 'react';
import { CDCConfig, MirrorSetter, TableMapRow } from '../../../dto/MirrorsDTO';
import { IsQueuePeer, fetchPublications } from '../handlers';
import { AdvancedSettingType, MirrorSetting } from '../helpers/common';
import CDCField from './fields';
import TableMapping from './tablemapping';

interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: CDCConfig;
  destinationType: DBType;
  sourceType: DBType;
  setter: MirrorSetter;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

export const defaultSyncMode = (dtype: DBType | undefined) => {
  switch (dtype) {
    case DBType.POSTGRES:
      return 'Copy with Binary';
    case DBType.SNOWFLAKE:
    case DBType.BIGQUERY:
    case DBType.S3:
      return 'AVRO';
    default:
      return 'Copy with Binary';
  }
};

export default function CDCConfigForm({
  settings,
  mirrorConfig,
  destinationType,
  sourceType,
  setter,
  rows,
  setRows,
}: MirrorConfigProps) {
  const [publications, setPublications] = useState<string[]>();
  const [pubLoading, setPubLoading] = useState(true);
  const [show, setShow] = useState(false);
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean = val;
    setting.stateHandler(stateVal, setter);
  };

  const normalSettings = useMemo(
    () =>
      settings!.filter(
        (setting) =>
          !(
            (IsQueuePeer(destinationType) &&
              setting.advanced === AdvancedSettingType.QUEUE) ||
            setting.advanced === AdvancedSettingType.ALL
          )
      ),
    [settings, destinationType]
  );

  const advancedSettings = useMemo(() => {
    return settings!
      .map((setting) => {
        if (
          IsQueuePeer(destinationType) &&
          setting.advanced === AdvancedSettingType.QUEUE
        ) {
          setting.stateHandler(600, setter);
          return { ...setting, default: 600 };
        }
        if (setting.advanced === AdvancedSettingType.ALL) {
          return setting;
        }
      })
      .filter((setting) => setting !== undefined);
  }, [settings, destinationType, setter]);

  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    const isQueue = IsQueuePeer(destinationType);
    if (
      (label.includes('snapshot') && mirrorConfig.doInitialSnapshot !== true) ||
      (label === 'replication slot name' &&
        mirrorConfig.doInitialSnapshot === true) ||
      (label.includes('staging path') &&
        defaultSyncMode(destinationType) !== 'AVRO') ||
      (isQueue && label.includes('soft delete')) ||
      (destinationType === DBType.EVENTHUBS &&
        (label.includes('initial copy') ||
          label.includes('initial load') ||
          label.includes('snapshot'))) ||
      ((sourceType !== DBType.POSTGRES ||
        destinationType !== DBType.POSTGRES) &&
        label.includes('type system')) ||
      (destinationType !== DBType.BIGQUERY && label.includes('column name')) ||
      (label.includes('soft delete') &&
        ![DBType.BIGQUERY, DBType.POSTGRES, DBType.SNOWFLAKE].includes(
          destinationType ?? DBType.UNRECOGNIZED
        ))
    ) {
      return false;
    }
    return true;
  };

  useEffect(() => {
    setPubLoading(true);
    fetchPublications(mirrorConfig.sourceName ?? '').then((pubs) => {
      setPublications(pubs);
      setPubLoading(false);
    });
  }, [mirrorConfig.sourceName]);

  if (mirrorConfig.sourceName && mirrorConfig.destinationName)
    return (
      <>
        {normalSettings!.map(
          (setting, id) =>
            paramDisplayCondition(setting!) && (
              <CDCField
                key={id}
                handleChange={handleChange}
                setting={setting!}
                options={
                  setting?.label === 'Publication Name'
                    ? publications
                    : undefined
                }
                optionsLoading={pubLoading}
              />
            )
        )}
        <Button
          className='IconButton'
          aria-label='collapse'
          onClick={() => {
            setShow((prev) => !prev);
          }}
          style={{
            width: '13em',
            height: '2.5em',
            marginTop: '2rem',
            marginBottom: '1rem',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <h3 style={{ marginRight: '10px' }}>Advanced Settings</h3>
            <Icon name={`keyboard_double_arrow_${show ? 'up' : 'down'}`} />
          </div>
        </Button>

        {show &&
          advancedSettings!.map((setting) => {
            return (
              paramDisplayCondition(setting!) && (
                <CDCField
                  key={setting?.label}
                  handleChange={handleChange}
                  setting={setting!}
                />
              )
            );
          })}

        <TableMapping
          sourcePeerName={mirrorConfig.sourceName}
          rows={rows}
          setRows={setRows}
          peerType={destinationType}
          omitAdditionalTablesMapping={new Map<string, string[]>()}
        />
      </>
    );
}
