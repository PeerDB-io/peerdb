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
import { Dispatch, SetStateAction, useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { InfoPopover } from '../../../../components/InfoPopover';
import { fetchAllTables, fetchColumns } from '../handlers';
import { MirrorSetting } from '../helpers/common';
import UpsertColsDisplay from './upsertcols';

interface QRepConfigProps {
  settings: MirrorSetting[];
  mirrorConfig: QRepConfig;
  destinationType: DBType;
  setter: Dispatch<SetStateAction<QRepConfig>>;
  xmin?: boolean;
}

const WriteModes = ['Append', 'Upsert', 'Overwrite'].map((value) => ({
  label: value,
  value,
}));
const allowedTypesForWatermarkColumn = [
  'smallint',
  'integer',
  'bigint',
  'timestamp without time zone',
  'timestamp with time zone',
];

export default function QRepConfigForm({
  settings,
  mirrorConfig,
  destinationType,
  setter,
  xmin,
}: QRepConfigProps) {
  const [sourceTables, setSourceTables] = useState<
    { value: string; label: string }[]
  >([]);
  const [allColumns, setAllColumns] = useState<
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
        !(
          destinationType.toString() === DBType[DBType.BIGQUERY] ||
          destinationType.toString() === DBType[DBType.SNOWFLAKE]
        )) ||
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
    fetchColumns(mirrorConfig.sourceName, schema, table, setLoading).then(
      (cols) => {
        const filteredCols = cols?.filter((col) =>
          allowedTypesForWatermarkColumn.includes(col.split(':')[1])
        );
        setAllColumns(
          cols.map((col) => ({
            value: col.split(':')[0],
            label: `${col.split(':')[0]} (${col.split(':')[1]})`,
          }))
        );
        setWatermarkColumns(
          filteredCols.map((col) => ({
            value: col.split(':')[0],
            label: `${col.split(':')[0]} (${col.split(':')[1]})`,
          }))
        );
      }
    );
  };

  const handleSourceChange = (
    val: string | undefined,
    setting: MirrorSetting
  ) => {
    if (val) {
      if (setting.label.includes('Table')) {
        loadColumnOptions(val);
      }
      handleChange(val, setting);
    }
  };

  useEffect(() => {
    fetchAllTables(mirrorConfig.sourceName).then((tables) =>
      setSourceTables(tables?.map((table) => ({ value: table, label: table })))
    );
  }, [mirrorConfig.sourceName]);

  return (
    <>
      {mirrorConfig.sourceName ? (
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
                      checked={setting.checked!(mirrorConfig)}
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
                          columns={allColumns}
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
                          onChange={(val, _action) =>
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
