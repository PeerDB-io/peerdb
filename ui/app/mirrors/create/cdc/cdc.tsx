'use client';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Dispatch, SetStateAction, useEffect, useMemo, useState } from 'react';
import { CDCConfig, MirrorSetter, TableMapRow } from '../../../dto/MirrorsDTO';
import { IsQueuePeer, fetchPublications } from '../handlers';
import { MirrorSetting } from '../helpers/common';
import CDCField from './fields';
import TableMapping from './tablemapping';

interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: CDCConfig;
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

  const normalSettings = useMemo(() => {
    return settings.filter((setting) => setting.advanced != true);
  }, [settings]);

  const advancedSettings = useMemo(() => {
    return settings.filter((setting) => setting.advanced == true);
  }, [settings]);

  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    const isQueue = IsQueuePeer(mirrorConfig.destination?.type);
    if (
      (label.includes('snapshot') && mirrorConfig.doInitialSnapshot !== true) ||
      (label === 'replication slot name' &&
        mirrorConfig.doInitialSnapshot === true &&
        !isQueue) ||
      (label.includes('staging path') &&
        defaultSyncMode(mirrorConfig.destination?.type) !== 'AVRO') ||
      (isQueue &&
        (label.includes('initial copy') ||
          label.includes('initial load') ||
          label.includes('soft delete') ||
          label.includes('snapshot')))
    ) {
      return false;
    }
    return true;
  };

  useEffect(() => {
    setPubLoading(true);
    fetchPublications(mirrorConfig.source?.name || '').then((pubs) => {
      setPublications(pubs);
      setPubLoading(false);
    });
  }, [mirrorConfig.source?.name]);

  if (mirrorConfig.source != undefined && mirrorConfig.destination != undefined)
    return (
      <>
        {normalSettings.map((setting, id) => {
          return (
            paramDisplayCondition(setting) && (
              <CDCField
                key={id}
                handleChange={handleChange}
                setting={setting}
                options={
                  setting.label === 'Publication Name'
                    ? publications
                    : undefined
                }
                publicationsLoading={pubLoading}
              />
            )
          );
        })}
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
          advancedSettings.map((setting, id) => {
            return (
              paramDisplayCondition(setting) && (
                <CDCField
                  key={setting.label}
                  handleChange={handleChange}
                  setting={setting}
                />
              )
            );
          })}

        <TableMapping
          sourcePeerName={mirrorConfig.source?.name}
          rows={rows}
          setRows={setRows}
          peerType={mirrorConfig.destination?.type}
          omitAdditionalTablesMapping={new Map<string, string[]>()}
          allowNoCDCTables={mirrorConfig.initialSnapshotOnly}
        />
      </>
    );
}
