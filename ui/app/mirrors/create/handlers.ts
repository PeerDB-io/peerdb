import { UCreateMirrorResponse } from '@/app/dto/MirrorsDTO';
import {
  UColumnsResponse,
  UPublicationsResponse,
  USchemasResponse,
  UTablesAllResponse,
  UTablesResponse,
} from '@/app/dto/PeersDTO';
import {
  FlowConnectionConfigs,
  QRepConfig,
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

const CDCCheck = (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig
) => {
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    const flowNameErr = flowNameValid.error.issues[0].message;
    return flowNameErr;
  }

  const tableNameMapping = reformattedTableMapping(rows);
  const fieldErr = validateCDCFields(tableNameMapping, config);
  if (fieldErr) {
    return fieldErr;
  }

  config['tableMappings'] = tableNameMapping as TableMapping[];
  config['flowJobName'] = flowJobName;

  if (config.doInitialSnapshot == false && config.initialSnapshotOnly == true) {
    return 'Initial Snapshot Only cannot be true if Initial Snapshot is false.';
  }

  if (config.doInitialSnapshot == true && config.replicationSlotName !== '') {
    config.replicationSlotName = '';
  }

  return '';
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
export const reformattedTableMapping = (
  tableMapping: TableMapRow[]
): TableMapping[] => {
  const mapping = tableMapping
    .filter((row) => row?.selected === true && row?.canMirror === true)
    .map((row) => ({
      sourceTableIdentifier: row.source,
      destinationTableIdentifier: row.destination,
      partitionKey: row.partitionKey,
      exclude: Array.from(row.exclude),
    }));
  return mapping;
};

export const handleCreateCDC = async (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig,
  notify: (msg: string, ok?: boolean) => void,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback
) => {
  const err = CDCCheck(flowJobName, rows, config);
  if (err != '') {
    notify(err);
    return;
  }

  setLoading(true);
  const statusMessage = await fetch('/api/mirrors/cdc', {
    method: 'POST',
    body: JSON.stringify({
      config,
    }),
  }).then((res) => res.json());
  if (!statusMessage.created) {
    notify(statusMessage.message || 'Unable to create mirror.');
    setLoading(false);
    return;
  }
  notify('CDC Mirror created successfully', true);
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

  if (config.sourcePeer?.snowflakeConfig) {
    if (config.watermarkTable == '') {
      notify('Please fill in the source table');
      return;
    }
    if (config.destinationTableIdentifier == '') {
      notify('Please fill in the destination table');
      return;
    }
  } else {
    const fieldErr = validateQRepFields(config.query, config);
    if (fieldErr) {
      notify(fieldErr);
      return;
    }
  }
  config.flowJobName = flowJobName;
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
    cache: 'no-store',
  }).then((res) => res.json());
  return schemasRes.schemas;
};

const getDefaultDestinationTable = (
  peerType: DBType,
  schemaName: string,
  tableName: string
) => {
  if (
    peerType.toString() == 'BIGQUERY' ||
    dBTypeToJSON(peerType) == 'BIGQUERY'
  ) {
    return tableName;
  }
  if (
    peerType.toString() == 'CLICKHOUSE' ||
    dBTypeToJSON(peerType) == 'CLICKHOUSE'
  ) {
    return `${schemaName}_${tableName}`;
  }
  return `${schemaName}.${tableName}`;
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
    cache: 'no-store',
  }).then((res) => res.json());

  let tables: TableMapRow[] = [];
  const tableRes = tablesRes.tables;
  if (tableRes) {
    for (const tableObject of tableRes) {
      // setting defaults:
      // for bigquery, tables are not schema-qualified
      const dstName = getDefaultDestinationTable(
        peerType!,
        schemaName,
        tableObject.tableName
      );
      tables.push({
        schema: schemaName,
        source: `${schemaName}.${tableObject.tableName}`,
        destination: dstName,
        partitionKey: '',
        exclude: new Set(),
        selected: false,
        canMirror: tableObject.canMirror,
        tableSize: tableObject.tableSize,
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
    cache: 'no-store',
  }).then((res) => res.json());
  setLoading(false);
  return columnsRes.columns;
};

export const fetchAllTables = async (peerName: string, peerType?: DBType) => {
  if (peerName?.length === 0) return [];
  const tablesRes: UTablesAllResponse = await fetch('/api/peers/tables/all', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
      peerType,
    }),
    cache: 'no-store',
  }).then((res) => res.json());
  return tablesRes.tables;
};

export const handleValidateCDC = async (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig,
  notify: (msg: string, ok?: boolean) => void,
  setLoading: Dispatch<SetStateAction<boolean>>
) => {
  setLoading(true);
  const err = CDCCheck(flowJobName, rows, config);
  if (err != '') {
    notify(err);
    setLoading(false);
    return;
  }
  const status = await fetch('/api/mirrors/cdc/validate', {
    method: 'POST',
    body: JSON.stringify({
      config,
    }),
  })
    .then((res) => res.json())
    .catch((e) => console.log(e));
  if (!status.ok) {
    notify(status.message || 'Mirror is invalid');
    setLoading(false);
    return;
  }
  notify('CDC Mirror is valid', true);
  setLoading(false);
};

export const fetchPublications = async (peerName: string) => {
  const publicationsRes: UPublicationsResponse = await fetch(
    '/api/peers/publications',
    {
      method: 'POST',
      body: JSON.stringify({
        peerName,
      }),
      cache: 'no-store',
    }
  ).then((res) => res.json());
  return publicationsRes.publicationNames;
};
