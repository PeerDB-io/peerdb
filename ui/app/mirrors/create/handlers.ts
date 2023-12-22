import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  UColumnsResponse,
  USchemasResponse,
  UTablesResponse,
} from '@/app/dto/PeersDTO';
import {
  FlowConnectionConfigs,
  QRepConfig,
  QRepSyncMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { DBType, Peer, dBTypeToJSON } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';
import { CDCConfig, TableMapRow } from '../../dto/MirrorsDTO';
import {
  cdcSchema,
  flowNameSchema,
  qrepSchema,
  tableMappingSchema,
} from './schema';

export const handlePeer = (
  peer: Peer | null,
  peerEnd: 'src' | 'dst',
  setConfig: (value: SetStateAction<FlowConnectionConfigs | QRepConfig>) => void
) => {
  if (!peer) return;
  if (peerEnd === 'dst') {
    if (peer.type === DBType.POSTGRES) {
      setConfig((curr) => {
        return {
          ...curr,
          cdcSyncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
          snapshotSyncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
          syncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
        };
      });
    } else if (
      peer.type === DBType.SNOWFLAKE ||
      peer.type === DBType.BIGQUERY
    ) {
      setConfig((curr) => {
        return {
          ...curr,
          cdcSyncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
          snapshotSyncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
          syncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
        };
      });
    }
    setConfig((curr) => ({
      ...curr,
      destination: peer,
      destinationPeer: peer,
    }));
  } else {
    setConfig((curr) => ({
      ...curr,
      source: peer,
      sourcePeer: peer,
    }));
  }
};

const validateCDCFields = (
  tableMapping: (
    | {
        sourceTableIdentifier: string;
        destinationTableIdentifier: string;
        partitionKey: string;
        exclude: string[];
      }
    | undefined
  )[],
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

interface TableMapping {
  sourceTableIdentifier: string;
  destinationTableIdentifier: string;
  partitionKey: string;
  exclude: string[];
}
const reformattedTableMapping = (tableMapping: TableMapRow[]) => {
  const mapping = tableMapping
    .map((row) => {
      if (row?.selected === true) {
        return {
          sourceTableIdentifier: row.source,
          destinationTableIdentifier: row.destination,
          partitionKey: row.partitionKey,
          exclude: row.exclude,
        };
      }
    })
    .filter(Boolean);
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
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    const flowNameErr = flowNameValid.error.issues[0].message;
    setMsg({ ok: false, msg: flowNameErr });
    return;
  }

  const tableNameMapping = reformattedTableMapping(rows);
  const isValid = validateCDCFields(tableNameMapping, setMsg, config);
  if (!isValid) return;

  config['tableMappings'] = tableNameMapping as TableMapping[];
  config['flowJobName'] = flowJobName;

  if (config.destination?.type == DBType.POSTGRES) {
    config.cdcSyncMode = QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    config.snapshotSyncMode = QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
  } else {
    config.cdcSyncMode = QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO;
    config.snapshotSyncMode = QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO;
  }
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

const quotedWatermarkTable = (watermarkTable: string): string => {
  if (watermarkTable.includes('.')) {
    const [schema, table] = watermarkTable.split('.');
    return `"${schema}"."${table}"`;
  } else {
    return `"${watermarkTable}"`;
  }
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
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    const flowNameErr = flowNameValid.error.issues[0].message;
    setMsg({ ok: false, msg: flowNameErr });
    return;
  }

  if (xmin == true) {
    config.watermarkColumn = 'xmin';
    config.query = `SELECT * FROM ${quotedWatermarkTable(
      config.watermarkTable
    )}`;
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

  if (config.destinationPeer?.type == DBType.POSTGRES) {
    config.syncMode = QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
  } else {
    config.syncMode = QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO;
  }

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

export const fetchSchemas = async (peerName: string) => {
  const schemasRes: USchemasResponse = await fetch('/api/peers/schemas', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
    }),
  }).then((res) => res.json());
  return schemasRes.schemas;
};

export const fetchTables = async (
  peerName: string,
  schemaName: string,
  peerType?: DBType
) => {
  if (schemaName.length === 0) return [];
  const tablesRes: UTablesResponse = await fetch('/api/peers/tables', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
      schemaName,
    }),
  }).then((res) => res.json());

  let tables = [];
  const tableNames = tablesRes.tables;
  if (tableNames) {
    for (const tableName of tableNames) {
      // setting defaults:
      // for bigquery, tables are not schema-qualified
      const dstName =
        peerType != undefined && dBTypeToJSON(peerType) == 'BIGQUERY'
          ? tableName
          : `${schemaName}.${tableName}`;

      tables.push({
        schema: schemaName,
        source: `${schemaName}.${tableName}`,
        destination: dstName,
        partitionKey: '',
        exclude: [],
        selected: false,
      });
    }
  }
  return tables;
};

export const fetchColumns = async (
  peerName: string,
  schemaName: string,
  tableName: string,
  setLoading: Dispatch<SetStateAction<boolean>>
) => {
  if (peerName?.length === 0) return [];
  setLoading(true);
  const columnsRes: UColumnsResponse = await fetch('/api/peers/columns', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
      schemaName,
      tableName,
    }),
  }).then((res) => res.json());
  setLoading(false);
  return columnsRes.columns;
};

export const fetchAllTables = async (peerName: string) => {
  if (peerName?.length === 0) return [];
  const tablesRes: UTablesResponse = await fetch('/api/peers/tables/all', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
    }),
  }).then((res) => res.json());
  return tablesRes.tables;
};
