'use client';
import { QRepSyncMode } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Dispatch, SetStateAction, useMemo, useState } from 'react';
import { CDCConfig, MirrorSetter, TableMapRow } from '../../../dto/MirrorsDTO';
import { MirrorSetting } from '../helpers/common';
import CDCField from './fields';
import GuideForDestinationSetup from './guide';
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
  const [show, setShow] = useState(false);
  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | QRepSyncMode = val;
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
    if (
      (label.includes('snapshot') && mirrorConfig.doInitialSnapshot !== true) ||
      (label === 'replication slot name' &&
        mirrorConfig.doInitialSnapshot === true) ||
      (label.includes('staging path') &&
        defaultSyncMode(mirrorConfig.destination?.type) !== 'AVRO')
    ) {
      return false;
    }
    return true;
  };

  if (mirrorConfig.source != undefined && mirrorConfig.destination != undefined)
    return (
      <>
        <GuideForDestinationSetup dstPeerType={mirrorConfig.destination.type} />
        {normalSettings.map((setting, id) => {
          return (
            paramDisplayCondition(setting) && (
              <CDCField
                key={id}
                handleChange={handleChange}
                setting={setting}
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
              <CDCField
                key={id}
                handleChange={handleChange}
                setting={setting}
              />
            );
          })}

        <TableMapping
          sourcePeerName={mirrorConfig.source?.name}
          rows={rows}
          setRows={setRows}
          peerType={mirrorConfig.destination?.type}
        />
      </>
    );
}
