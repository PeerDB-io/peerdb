'use client';
import { Dispatch, SetStateAction, useEffect, useMemo, useState } from 'react';

import { TableMapping } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { CDCConfig, TableMapRow } from '../../../dto/MirrorsDTO';
import { IsEventhubsPeer, IsQueuePeer, fetchPublications } from '../handlers';
import { AdvancedSettingType, MirrorSetting } from '../helpers/common';
import CDCField from './fields';
import TablePicker from './tablemapping';

interface MirrorConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: CDCConfig;
  setter: Dispatch<SetStateAction<CDCConfig>>;
  destinationType: DBType;
  sourceType: DBType;
  rows: TableMapRow[];
  setRows: Dispatch<SetStateAction<TableMapRow[]>>;
}

export default function CDCConfigForm({
  settings,
  mirrorConfig,
  setter,
  destinationType,
  sourceType,
  rows,
  setRows,
}: MirrorConfigProps) {
  const [publications, setPublications] = useState<string[]>();
  const [show, setShow] = useState(false);
  const [loading, setLoading] = useState(false);
  const [scriptingEnabled, setScriptingEnabled] = useState(false);
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean = val;
    setting.stateHandler(stateVal, setter);
  };

  const getScriptingEnabled = async () => {
    const response = await fetch('/api/mirror-types/validation/scripting');
    const data = await response.json();
    setScriptingEnabled(data);
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
    if (
      (label.includes('snapshot') && mirrorConfig.doInitialSnapshot !== true) ||
      (label === 'replication slot name' &&
        mirrorConfig.doInitialSnapshot === true) ||
      (label.includes('staging path') &&
        !(
          destinationType.toString() === DBType[DBType.BIGQUERY] ||
          destinationType.toString() === DBType[DBType.SNOWFLAKE]
        )) ||
      (IsEventhubsPeer(destinationType) &&
        (label.includes('initial copy') ||
          label.includes('initial load') ||
          label.includes('snapshot'))) ||
      ((sourceType.toString() !== DBType[DBType.POSTGRES] ||
        destinationType.toString() !== DBType[DBType.POSTGRES]) &&
        label.includes('type system')) ||
      (destinationType.toString() !== DBType[DBType.BIGQUERY] &&
        label.includes('column name')) ||
      (label.includes('soft delete') &&
        !(
          destinationType.toString() === DBType[DBType.POSTGRES] ||
          destinationType.toString() === DBType[DBType.BIGQUERY] ||
          destinationType.toString() === DBType[DBType.SNOWFLAKE]
        )) ||
      (!scriptingEnabled &&
        label.includes('script') &&
        destinationType.toString() === DBType[DBType.CLICKHOUSE]) ||
      (label.includes('system') &&
        destinationType.toString() !== DBType[DBType.POSTGRES])
    ) {
      return false;
    }
    return true;
  };

  useEffect(() => {
    setLoading(true);
    fetchPublications(mirrorConfig.sourceName ?? '').then((pubs) => {
      setPublications(pubs);
    });
    getScriptingEnabled();
    setLoading(false);
  }, [mirrorConfig.sourceName, mirrorConfig.initialSnapshotOnly]);

  if (loading) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }
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
          advancedSettings!.map(
            (setting) =>
              paramDisplayCondition(setting!) && (
                <CDCField
                  key={setting?.label}
                  handleChange={handleChange}
                  setting={setting!}
                />
              )
          )}

        <TablePicker
          sourcePeerName={mirrorConfig.sourceName}
          rows={rows}
          setRows={setRows}
          peerType={destinationType}
          alreadySelectedTablesMapping={new Map<string, TableMapping[]>()}
          initialLoadOnly={mirrorConfig.initialSnapshotOnly}
        />
      </>
    );
}
