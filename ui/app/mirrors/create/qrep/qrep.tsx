'use client';
import SelectTheme from '@/app/styles/select';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepConfig, QRepWriteType } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithSwitch, RowWithTextField } from '@/lib/Layout';
import { Switch } from '@/lib/Switch';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { InfoPopover } from '../../../../components/InfoPopover';
import { MirrorSetter } from '../../../dto/MirrorsDTO';
import { defaultSyncMode } from '../cdc/cdc';
import { fetchAllTables, fetchColumns } from '../handlers';
import { MirrorSetting, blankQRepSetting } from '../helpers/common';
import QRepQuery from './query';
import UpsertColsDisplay from './upsertcols';

interface QRepConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: QRepConfig;
  setter: MirrorSetter;
  xmin?: boolean;
}

const WriteModes = ['Append', 'Upsert', 'Overwrite'].map((value) => ({
  label: value,
  value,
}));

export default function QRepConfigForm({
  settings,
  mirrorConfig,
  setter,
  xmin,
}: QRepConfigProps) {
  const [sourceTables, setSourceTables] = useState<
    { value: string; label: string }[]
  >([]);
  const [watermarkColumns, setWatermarkColumns] = useState<
    { value: string; label: string }[]
  >([]);

  const [loading, setLoading] = useState(false);

  const handleChange = (val: string | boolean, setting: MirrorSetting) => {
    let stateVal: string | boolean | QRepWriteType | string[] = val;
    if (setting.label.includes('Write Type')) {
      switch (val) {
        case 'Upsert':
          stateVal = QRepWriteType.QREP_WRITE_MODE_UPSERT;
          break;
        case 'Overwrite':
          stateVal = QRepWriteType.QREP_WRITE_MODE_OVERWRITE;
          break;
        default:
          stateVal = QRepWriteType.QREP_WRITE_MODE_APPEND;
          break;
      }
    } else if (setting.label === 'Upsert Key Columns') {
      const columns = val as string;
      stateVal = columns.split(',').map((item) => item.trim());
    }
    setting.stateHandler(stateVal, setter);
  };

  const paramDisplayCondition = (setting: MirrorSetting) => {
    const label = setting.label.toLowerCase();
    if (
      (label.includes('upsert') &&
        mirrorConfig.writeMode?.writeType !=
          QRepWriteType.QREP_WRITE_MODE_UPSERT) ||
      (label.includes('staging') &&
        defaultSyncMode(mirrorConfig.destinationPeer?.type) !== 'AVRO') ||
      (label.includes('watermark column') && xmin) ||
      (label.includes('initial copy') && xmin)
    ) {
      return false;
    }
    return true;
  };

  const loadColumnOptions = (tableIdentifier: string) => {
    const schema = tableIdentifier.split('.')[0];
    const table = tableIdentifier.split('.')[1];
    fetchColumns(
      mirrorConfig.sourcePeer?.name ?? '',
      schema,
      table,
      setLoading
    ).then((cols) =>
      setWatermarkColumns(
        cols?.map((col) => ({
          value: col.split(':')[0],
          label: `${col.split(':')[0]} (${col.split(':')[1]})`,
        }))
      )
    );
  };

  const handleSourceChange = (
    val: string | undefined,
    setting: MirrorSetting
  ) => {
    if (val) {
      if (setting.label === 'Table') {
        if (mirrorConfig.destinationPeer?.type === DBType.BIGQUERY) {
          setter((curr) => ({
            ...curr,
            destinationTableIdentifier: val.split('.')[1],
          }));
        } else {
          setter((curr) => ({
            ...curr,
            destinationTableIdentifier: val,
          }));
        }
        loadColumnOptions(val);
      }
      handleChange(val, setting);
    }
  };

  useEffect(() => {
    fetchAllTables(
      mirrorConfig.sourcePeer?.name ?? '',
      mirrorConfig.sourcePeer?.type
    ).then((tables) =>
      setSourceTables(tables?.map((table) => ({ value: table, label: table })))
    );
  }, [mirrorConfig.sourcePeer]);

  useEffect(() => {
    // set defaults
    setter((curr) => ({ ...curr, ...blankQRepSetting }));
  }, [setter]);
  return (
    <>
      <QRepQuery
        query={mirrorConfig.query}
        setter={(val: string) => {
          setter((curr) => ({
            ...curr,
            query: val,
          }));
        }}
      />
      {mirrorConfig.sourcePeer?.name ? (
        settings.map((setting, id) => {
          return (
            paramDisplayCondition(setting) &&
            (setting.type === 'switch' ? (
              <RowWithSwitch
                key={id}
                label={<Label>{setting.label}</Label>}
                action={
                  <div
                    style={{
                      display: 'flex',
                      flexDirection: 'row',
                      alignItems: 'center',
                    }}
                  >
                    <Switch
                      checked={
                        setting.label === 'Create Destination Table'
                          ? mirrorConfig.setupWatermarkTableOnDestination
                          : setting.label === 'Initial Copy Only'
                            ? mirrorConfig.initialCopyOnly
                            : mirrorConfig.dstTableFullResync
                      }
                      onCheckedChange={(state: boolean) =>
                        handleChange(state, setting)
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
            ) : setting.type === 'select' ? (
              <RowWithSelect
                key={id}
                label={
                  <Label>
                    {setting.label}
                    {RequiredIndicator(setting.required)}
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
                    <div style={{ width: '100%' }}>
                      {setting.label.includes('Write') ? (
                        <ReactSelect
                          placeholder='Select a write mode'
                          onChange={(val) =>
                            val && handleChange(val.value, setting)
                          }
                          options={WriteModes}
                          theme={SelectTheme}
                        />
                      ) : setting.label === 'Upsert Key Columns' ? (
                        <UpsertColsDisplay
                          columns={watermarkColumns}
                          loading={loading}
                          setter={setter}
                          setting={setting}
                        />
                      ) : (
                        <ReactSelect
                          placeholder={
                            setting.label.includes('Column')
                              ? 'Select a column'
                              : 'Select a table'
                          }
                          onChange={(val, action) =>
                            handleSourceChange(val?.value, setting)
                          }
                          isLoading={loading}
                          options={
                            setting.label.includes('Column')
                              ? watermarkColumns
                              : sourceTables
                          }
                          theme={SelectTheme}
                        />
                      )}
                    </div>
                    {setting.tips && (
                      <InfoPopover
                        tips={setting.tips}
                        link={setting.helpfulLink}
                      />
                    )}
                  </div>
                }
              />
            ) : (
              <RowWithTextField
                key={id}
                label={
                  <Label>
                    {setting.label}
                    {setting.required && (
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
                      type={setting.type}
                      defaultValue={setting.default as string}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                        handleChange(e.target.value, setting)
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
            ))
          );
        })
      ) : (
        <Label as='label' style={{ color: 'gray', fontSize: 15 }}>
          Please select a source peer
        </Label>
      )}
    </>
  );
}
