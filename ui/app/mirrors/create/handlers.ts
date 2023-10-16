import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import { AppRouterInstance } from 'next/dist/shared/lib/app-router-context';
import { Dispatch, SetStateAction } from 'react';
import { MirrorConfig, TableMapRow } from '../types';
import { cdcSchema, tableMappingSchema } from './schema';

const validateFlowFields = (
  tableMapping: TableMapRow[],
  setMsg: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  config: MirrorConfig
): boolean => {
  let validationErr: string | undefined;
  const tablesValidity = tableMappingSchema.safeParse(tableMapping);
  if (!tablesValidity.success) {
    validationErr = tablesValidity.error.issues[0].message;
    setMsg({ ok: false, msg: validationErr });
    return false;
  }
  const configValidity = cdcSchema.safeParse(config);
  if (!configValidity.success) {
    validationErr = configValidity.error.issues[0].message;
    setMsg({ ok: false, msg: validationErr });
    return false;
  }
  setMsg({ ok: true, msg: '' });
  return true;
};

const reformattedTableMapping = (tableMapping: TableMapRow[]) => {
  const mapping = tableMapping.map((row) => {
    return {
      sourceTableIdentifier: row.source,
      destinationTableIdentifier: row.destination,
      partitionKey: '',
    };
  });
  return mapping;
};
export const handleCreate = async (
  flowJobName: string,
  rows: TableMapRow[],
  config: MirrorConfig,
  setMsg: Dispatch<
    SetStateAction<{
      ok: boolean;
      msg: string;
    }>
  >,
  setLoading: Dispatch<SetStateAction<boolean>>,
  router: AppRouterInstance
) => {
  if (!flowJobName) {
    setMsg({ ok: false, msg: 'Mirror name is required' });
    return;
  }
  const isValid = validateFlowFields(rows, setMsg, config);
  if (!isValid) return;
  const tableNameMapping = reformattedTableMapping(rows);
  config['tableMappings'] = tableNameMapping;
  config['flowJobName'] = flowJobName;
  setLoading(true);
  const statusMessage: UCreateMirrorResponse = await fetch('/api/mirrors/cdc', {
    method: 'POST',
    body: JSON.stringify({
      config,
    }),
  }).then((res) => res.json());
  if (!statusMessage.created) {
    setMsg({ ok: false, msg: 'unable to create mirror.' });
    setLoading(false);
    return;
  }
  setMsg({ ok: true, msg: 'CDC Mirror created successfully' });
  router.push('/mirrors');
  setLoading(false);
};
