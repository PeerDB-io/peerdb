import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  UColumnsResponse,
  USchemasResponse,
  UTablesAllResponse,
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
  config: CDCConfig
): string | undefined => {
  let validationErr: string | undefined;
  const tablesValidity = tableMappingSchema.safeParse(tableMapping);
  if (!tablesValidity.success) {
    validationErr = tablesValidity.error.issues[0].message;
  }
  const configValidity = cdcSchema.safeParse(config);
  if (!configValidity.success) {
    validationErr = configValidity.error.issues[0].message;
  }
  return validationErr;
};

const validateQRepFields = (
  query: string,
  config: QRepConfig
): string | undefined => {
  if (query.length < 5) {
    return 'Query is invalid';
  }
  let validationErr: string | undefined;
  const configValidity = qrepSchema.safeParse(config);
  if (!configValidity.success) {
    validationErr = configValidity.error.issues[0].message;
  }
  return validationErr;
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
  notify: (msg: string) => void,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback
) => {
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    const flowNameErr = flowNameValid.error.issues[0].message;
    notify(flowNameErr);
    return;
  }

  const tableNameMapping = reformattedTableMapping(rows);
  const fieldErr = validateCDCFields(tableNameMapping, config);
  if (fieldErr) {
    notify(fieldErr);
    return;
  }

  config['tableMappings'] = tableNameMapping as TableMapping[];
  config['flowJobName'] = flowJobName;

  if (config.destination?.type == DBType.POSTGRES) {
    config.cdcSyncMode = QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    config.snapshotSyncMode = QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
  } else {
    config.cdcSyncMode = QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO;
    config.snapshotSyncMode = QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO;
  }

  if (config.doInitialCopy == false && config.initialCopyOnly == true) {
    notify('Initial Copy Only cannot be true if Initial Copy is false.');
    return;
  }

  setLoading(true);
  const statusMessage: UCreateMirrorResponse = await fetch('/api/mirrors/cdc', {
    method: 'POST',
    body: JSON.stringify({
      config,
    }),
  }).then((res) => res.json());
  if (!statusMessage.created) {
    notify('unable to create mirror.');
    setLoading(false);
    return;
  }
  notify('CDC Mirror created successfully');
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
  notify: (msg: string) => void,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback,
  xmin?: boolean
) => {
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    const flowNameErr = flowNameValid.error.issues[0].message;
    notify(flowNameErr);
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
    (!config.writeMode?.upsertKeyColumns ||
      config.writeMode?.upsertKeyColumns.length == 0)
  ) {
    notify('For upsert mode, unique key columns cannot be empty.');
    return;
  }
  const fieldErr = validateQRepFields(query, config);
  if (fieldErr) {
    notify(fieldErr);
    return;
  }
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
    notify('unable to create mirror.');
    setLoading(false);
    return;
  }
  notify('Query Replication Mirror created successfully');
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

  let tables: TableMapRow[] = [];
  const tableRes = tablesRes.tables;
  if (tableRes) {
    for (const tableObject of tableRes) {
      // setting defaults:
      // for bigquery, tables are not schema-qualified
      const dstName =
        peerType != undefined && dBTypeToJSON(peerType) == 'BIGQUERY'
          ? tableObject.tableName
          : `${schemaName}.${tableObject.tableName}`;

      tables.push({
        schema: schemaName,
        source: `${schemaName}.${tableObject.tableName}`,
        destination: dstName,
        partitionKey: '',
        exclude: [],
        selected: false,
        canMirror: tableObject.canMirror,
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
  const tablesRes: UTablesAllResponse = await fetch('/api/peers/tables/all', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
    }),
  }).then((res) => res.json());
  return tablesRes.tables;
};
