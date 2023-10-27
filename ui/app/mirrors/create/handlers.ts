import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import { QRepConfig, QRepWriteType } from '@/grpc_generated/flow';
import { Dispatch, SetStateAction } from 'react';
import { CDCConfig, TableMapRow } from '../../dto/MirrorsDTO';
import { cdcSchema, qrepSchema, tableMappingSchema } from './schema';

const validateCDCFields = (
  tableMapping: TableMapRow[],
  setMsg: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  config: CDCConfig
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

const validateQRepFields = (
  query: string,
  setMsg: Dispatch<SetStateAction<{ ok: boolean; msg: string }>>,
  config: QRepConfig
): boolean => {
  if (query.length < 5) {
    setMsg({ ok: false, msg: 'Query is invalid' });
    return false;
  }

  let validationErr: string | undefined;
  const configValidity = qrepSchema.safeParse(config);
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

export const handleCreateCDC = async (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig,
  setMsg: Dispatch<
    SetStateAction<{
      ok: boolean;
      msg: string;
    }>
  >,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback
) => {
  if (!flowJobName) {
    setMsg({ ok: false, msg: 'Mirror name is required' });
    return;
  }
  const isValid = validateCDCFields(rows, setMsg, config);
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
  route();
  setLoading(false);
};

export const handleCreateQRep = async (
  flowJobName: string,
  query: string,
  config: QRepConfig,
  setMsg: Dispatch<
    SetStateAction<{
      ok: boolean;
      msg: string;
    }>
  >,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback,
  xmin?: boolean
) => {
  if (!flowJobName) {
    setMsg({ ok: false, msg: 'Mirror name is required' });
    return;
  }
  if (xmin == true) {
    config.watermarkColumn = 'xmin';
    config.query = `SELECT * FROM ${config.watermarkTable} WHERE xmin::text::bigint BETWEEN {{.start}} AND {{.end}}`;
    query = config.query;
    config.initialCopyOnly = false;
  }

  if (
    config.writeMode?.writeType == QRepWriteType.QREP_WRITE_MODE_UPSERT &&
    !config.writeMode?.upsertKeyColumns
  ) {
    setMsg({
      ok: false,
      msg: 'For upsert mode, unique key columns cannot be empty.',
    });
    return;
  }
  const isValid = validateQRepFields(query, setMsg, config);
  if (!isValid) return;
  config.flowJobName = flowJobName;
  config.query = query;
  setLoading(true);
  const statusMessage: UCreateMirrorResponse = await fetch(
    '/api/mirrors/qrep',
    {
      method: 'POST',
      body: JSON.stringify({
        config,
      }),
    }
  ).then((res) => res.json());
  if (!statusMessage.created) {
    setMsg({ ok: false, msg: 'unable to create mirror.' });
    setLoading(false);
    return;
  }
  setMsg({ ok: true, msg: 'Query Replication Mirror created successfully' });
  route();
  setLoading(false);
};

const fetchSchemas = (peerName:string) => {
    
}