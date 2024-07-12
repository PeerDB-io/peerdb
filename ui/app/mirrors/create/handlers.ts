import {
  UPublicationsResponse,
  UTablesAllResponse,
  UTablesResponse,
} from '@/app/dto/PeersDTO';
import { notifyErr } from '@/app/utils/notify';
import QRepQueryTemplate from '@/app/utils/qreptemplate';
import { DBTypeToGoodText } from '@/components/PeerTypeComponent';
import {
  FlowConnectionConfigs,
  QRepConfig,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { DBType, dBTypeToJSON } from '@/grpc_generated/peers';
import {
  CreateCDCFlowRequest,
  CreateQRepFlowRequest,
  PeerSchemasResponse,
  TableColumnsResponse,
} from '@/grpc_generated/route';
import { Dispatch, SetStateAction } from 'react';
import { CDCConfig, TableMapRow } from '../../dto/MirrorsDTO';
import {
  cdcSchema,
  flowNameSchema,
  qrepSchema,
  tableMappingSchema,
} from './schema';
export const IsQueuePeer = (peerType?: DBType): boolean => {
  return (
    !!peerType &&
    (peerType === DBType.KAFKA ||
      peerType === DBType.PUBSUB ||
      peerType === DBType.EVENTHUBS)
  );
};

export const IsEventhubsPeer = (peerType?: DBType): boolean => {
  return (
    (!!peerType && peerType === DBType.EVENTHUBS) ||
    peerType?.toString() === DBType[DBType.EVENTHUBS]
  );
};

const ValidSchemaQualifiedTarget = (
  peerType: DBType,
  tableName: string
): boolean => {
  const schemaRequiredPeer =
    peerType === DBType.POSTGRES || peerType === DBType.SNOWFLAKE;
  if (!schemaRequiredPeer) {
    return true;
  }

  return !!tableName && tableName.includes('.') && !tableName.startsWith('.');
};

const CDCCheck = (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig,
  destinationType: DBType
) => {
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    return flowNameValid.error.issues[0].message;
  }

  const tableNameMapping = reformattedTableMapping(rows);
  const fieldErr = validateCDCFields(tableNameMapping, config, destinationType);
  if (fieldErr) {
    return fieldErr;
  }

  config.tableMappings = tableNameMapping as TableMapping[];
  config.flowJobName = flowJobName;

  if (IsEventhubsPeer(destinationType)) {
    config.doInitialSnapshot = false;
  }

  if (config.doInitialSnapshot == false && config.initialSnapshotOnly == true) {
    return 'Initial Snapshot Only cannot be true if Initial Snapshot is false.';
  }

  if (config.doInitialSnapshot == true && config.replicationSlotName !== '') {
    config.replicationSlotName = '';
  }

  if (IsQueuePeer(destinationType)) {
    config.softDeleteColName = '';
  }

  return '';
};

// check if table names are schema-qualified if applicable
const validateSchemaQualification = (
  tableMapping: (TableMapping | undefined)[],
  destinationType: DBType
): string => {
  for (const table of tableMapping) {
    if (
      !ValidSchemaQualifiedTarget(
        destinationType,
        table!.destinationTableIdentifier
      )
    ) {
      return `Destination table ${table?.destinationTableIdentifier} should be schema qualified`;
    }
  }
  return '';
};

const validateCDCFields = (
  tableMapping: (TableMapping | undefined)[],
  config: CDCConfig,
  destinationType: DBType
): string | undefined => {
  const tableQualificationErr = validateSchemaQualification(
    tableMapping,
    destinationType
  );
  if (tableQualificationErr) {
    return tableQualificationErr;
  }
  const tablesValidity = tableMappingSchema.safeParse(tableMapping);
  if (!tablesValidity.success) {
    return tablesValidity.error.issues[0].message;
  }

  const configValidity = cdcSchema.safeParse(config);
  if (!configValidity.success) {
    return configValidity.error.issues[0].message;
  }
};

const validateQRepFields = (
  query: string,
  config: QRepConfig
): string | undefined => {
  if (query.length < 5) {
    return 'Query is invalid';
  }
  const configValidity = qrepSchema.safeParse(config);
  if (!configValidity.success) {
    return configValidity.error.issues[0].message;
  }
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

const processCDCConfig = (a: CDCConfig): FlowConnectionConfigs => {
  const ret = a as FlowConnectionConfigs;
  if (a.disablePeerDBColumns) {
    ret.softDeleteColName = '';
    ret.syncedAtColName = '';
  }
  return ret;
};

export const handleCreateCDC = async (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig,
  destinationType: DBType,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback
) => {
  const err = CDCCheck(flowJobName, rows, config, destinationType);
  if (err) {
    notifyErr(err);
    return;
  }

  setLoading(true);
  const res = await fetch('/api/mirrors/cdc', {
    method: 'POST',
    body: JSON.stringify({
      connectionConfigs: processCDCConfig(config),
    } as CreateCDCFlowRequest),
  });
  if (!res.ok) {
    // I don't know why but if the order is reversed the error message is not shown
    setLoading(false);
    notifyErr((await res.json()).message || 'Unable to create mirror.');
    return;
  }
  setLoading(false);
  notifyErr('CDC Mirror created successfully', true);
  route();
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
  destinationType: DBType,
  setLoading: Dispatch<SetStateAction<boolean>>,
  route: RouteCallback,
  xmin?: boolean
) => {
  const flowNameValid = flowNameSchema.safeParse(flowJobName);
  if (!flowNameValid.success) {
    const flowNameErr = flowNameValid.error.issues[0].message;
    notifyErr(flowNameErr);
    return;
  }

  if (query === QRepQueryTemplate && !xmin) {
    notifyErr('Please fill in the query box');
    return;
  }

  if (
    !xmin &&
    config.writeMode?.writeType != QRepWriteType.QREP_WRITE_MODE_OVERWRITE &&
    !(query.includes('{{.start}}') && query.includes('{{.end}}'))
  ) {
    notifyErr(
      'Please include placeholders {{.start}} and {{.end}} in the query'
    );
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
    notifyErr('For upsert mode, unique key columns cannot be empty.');
    return;
  }
  const fieldErr = validateQRepFields(query, config);
  if (fieldErr) {
    notifyErr(fieldErr);
    return;
  }
  config.flowJobName = flowJobName;
  config.query = query;

  if (
    !ValidSchemaQualifiedTarget(
      destinationType,
      config.destinationTableIdentifier
    )
  ) {
    notifyErr(
      `Destination table should be schema qualified for ${DBTypeToGoodText(destinationType)} targets`
    );
    return;
  }

  setLoading(true);
  const res = await fetch('/api/mirrors/qrep', {
    method: 'POST',
    body: JSON.stringify({
      qrepConfig: config,
      createCatalogEntry: true,
    } as CreateQRepFlowRequest),
  });
  if (!res.ok) {
    setLoading(false);
    notifyErr((await res.json()).message || 'Unable to create mirror.');
    return;
  }
  setLoading(false);
  notifyErr('Query Replication Mirror created successfully');
  route();
};

export const fetchSchemas = async (peerName: string) => {
  const schemasRes: PeerSchemasResponse = await fetch('/api/peers/schemas', {
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
    if (schemaName.length === 0) {
      return tableName;
    }
    return `${schemaName}_${tableName}`;
  }

  if (
    peerType.toString() == 'CLICKHOUSE' ||
    dBTypeToJSON(peerType) == 'CLICKHOUSE'
  ) {
    if (schemaName.length === 0) {
      return tableName;
    }
    return `${schemaName}_${tableName}`;
  }

  if (
    peerType.toString() == 'EVENTHUBS' ||
    dBTypeToJSON(peerType) == 'EVENTHUBS'
  ) {
    return `<namespace>.${schemaName}_${tableName}.<partition_column>`;
  }

  if (schemaName.length === 0) {
    return tableName;
  }

  return `${schemaName}.${tableName}`;
};

export const fetchTables = async (
  peerName: string,
  schemaName: string,
  targetSchemaName: string,
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
        targetSchemaName,
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
  const columnsRes: TableColumnsResponse = await fetch('/api/peers/columns', {
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

export const fetchAllTables = async (peerName: string) => {
  if (peerName?.length === 0) return [];
  const tablesRes: UTablesAllResponse = await fetch('/api/peers/tables/all', {
    method: 'POST',
    body: JSON.stringify({
      peerName,
    }),
    cache: 'no-store',
  }).then((res) => res.json());
  return tablesRes.tables;
};

export const handleValidateCDC = async (
  flowJobName: string,
  rows: TableMapRow[],
  config: CDCConfig,
  destinationType: DBType,
  setLoading: Dispatch<SetStateAction<boolean>>
) => {
  setLoading(true);
  const err = CDCCheck(flowJobName, rows, config, destinationType);
  if (err) {
    notifyErr(err);
    setLoading(false);
    return;
  }
  const status = await fetch('/api/mirrors/cdc/validate', {
    method: 'POST',
    body: JSON.stringify({
      config: processCDCConfig(config),
    }),
  }).then((res) => res.json());

  if (!status.ok) {
    notifyErr(status.message || 'Mirror is invalid');
    setLoading(false);
    return;
  }
  notifyErr('CDC Mirror is valid', true);
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
