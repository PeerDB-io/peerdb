/* eslint-disable */
import Long from "long";
import _m0 from "protobufjs/minimal";
import { Timestamp } from "./google/protobuf/timestamp";
import { Peer } from "./peers";

export const protobufPackage = "peerdb_flow";

/** protos for qrep */
export enum QRepSyncMode {
  QREP_SYNC_MODE_MULTI_INSERT = 0,
  QREP_SYNC_MODE_STORAGE_AVRO = 1,
  UNRECOGNIZED = -1,
}

export function qRepSyncModeFromJSON(object: any): QRepSyncMode {
  switch (object) {
    case 0:
    case "QREP_SYNC_MODE_MULTI_INSERT":
      return QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT;
    case 1:
    case "QREP_SYNC_MODE_STORAGE_AVRO":
      return QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO;
    case -1:
    case "UNRECOGNIZED":
    default:
      return QRepSyncMode.UNRECOGNIZED;
  }
}

export function qRepSyncModeToJSON(object: QRepSyncMode): string {
  switch (object) {
    case QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT:
      return "QREP_SYNC_MODE_MULTI_INSERT";
    case QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO:
      return "QREP_SYNC_MODE_STORAGE_AVRO";
    case QRepSyncMode.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export enum QRepWriteType {
  QREP_WRITE_MODE_APPEND = 0,
  QREP_WRITE_MODE_UPSERT = 1,
  /** QREP_WRITE_MODE_OVERWRITE - only valid when initial_copy_true is set to true. TRUNCATES tables before reverting to APPEND. */
  QREP_WRITE_MODE_OVERWRITE = 2,
  UNRECOGNIZED = -1,
}

export function qRepWriteTypeFromJSON(object: any): QRepWriteType {
  switch (object) {
    case 0:
    case "QREP_WRITE_MODE_APPEND":
      return QRepWriteType.QREP_WRITE_MODE_APPEND;
    case 1:
    case "QREP_WRITE_MODE_UPSERT":
      return QRepWriteType.QREP_WRITE_MODE_UPSERT;
    case 2:
    case "QREP_WRITE_MODE_OVERWRITE":
      return QRepWriteType.QREP_WRITE_MODE_OVERWRITE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return QRepWriteType.UNRECOGNIZED;
  }
}

export function qRepWriteTypeToJSON(object: QRepWriteType): string {
  switch (object) {
    case QRepWriteType.QREP_WRITE_MODE_APPEND:
      return "QREP_WRITE_MODE_APPEND";
    case QRepWriteType.QREP_WRITE_MODE_UPSERT:
      return "QREP_WRITE_MODE_UPSERT";
    case QRepWriteType.QREP_WRITE_MODE_OVERWRITE:
      return "QREP_WRITE_MODE_OVERWRITE";
    case QRepWriteType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface TableNameMapping {
  sourceTableName: string;
  destinationTableName: string;
}

export interface RelationMessageColumn {
  flags: number;
  name: string;
  dataType: number;
}

export interface RelationMessage {
  relationId: number;
  relationName: string;
  columns: RelationMessageColumn[];
}

export interface TableMapping {
  sourceTableIdentifier: string;
  destinationTableIdentifier: string;
  partitionKey: string;
  exclude: string[];
}

export interface FlowConnectionConfigs {
  source: Peer | undefined;
  destination: Peer | undefined;
  flowJobName: string;
  tableSchema: TableSchema | undefined;
  tableMappings: TableMapping[];
  srcTableIdNameMapping: { [key: number]: string };
  tableNameSchemaMapping: { [key: string]: TableSchema };
  /**
   * This is an optional peer that will be used to hold metadata in cases where
   * the destination isn't ideal for holding metadata.
   */
  metadataPeer: Peer | undefined;
  maxBatchSize: number;
  doInitialCopy: boolean;
  publicationName: string;
  snapshotNumRowsPerPartition: number;
  /** max parallel workers is per table */
  snapshotMaxParallelWorkers: number;
  snapshotNumTablesInParallel: number;
  snapshotSyncMode: QRepSyncMode;
  cdcSyncMode: QRepSyncMode;
  snapshotStagingPath: string;
  cdcStagingPath: string;
  /** currently only works for snowflake */
  softDelete: boolean;
  replicationSlotName: string;
  /** the below two are for eventhub only */
  pushBatchSize: number;
  pushParallelism: number;
  /**
   * if true, then the flow will be resynced
   * create new tables with "_resync" suffix, perform initial load and then swap the new tables with the old ones
   * to be used after the old mirror is dropped
   */
  resync: boolean;
  softDeleteColName: string;
  syncedAtColName: string;
}

export interface FlowConnectionConfigs_SrcTableIdNameMappingEntry {
  key: number;
  value: string;
}

export interface FlowConnectionConfigs_TableNameSchemaMappingEntry {
  key: string;
  value: TableSchema | undefined;
}

export interface RenameTableOption {
  currentName: string;
  newName: string;
  tableSchema: TableSchema | undefined;
}

export interface RenameTablesInput {
  flowJobName: string;
  peer: Peer | undefined;
  renameTableOptions: RenameTableOption[];
  softDeleteColName?: string | undefined;
  syncedAtColName?: string | undefined;
}

export interface RenameTablesOutput {
  flowJobName: string;
}

export interface CreateTablesFromExistingInput {
  flowJobName: string;
  peer: Peer | undefined;
  newToExistingTableMapping: { [key: string]: string };
}

export interface CreateTablesFromExistingInput_NewToExistingTableMappingEntry {
  key: string;
  value: string;
}

export interface CreateTablesFromExistingOutput {
  flowJobName: string;
}

export interface SyncFlowOptions {
  batchSize: number;
  relationMessageMapping: { [key: number]: RelationMessage };
}

export interface SyncFlowOptions_RelationMessageMappingEntry {
  key: number;
  value: RelationMessage | undefined;
}

export interface NormalizeFlowOptions {
  batchSize: number;
}

export interface LastSyncState {
  checkpoint: number;
  lastSyncedAt: Date | undefined;
}

export interface StartFlowInput {
  lastSyncState: LastSyncState | undefined;
  flowConnectionConfigs: FlowConnectionConfigs | undefined;
  syncFlowOptions: SyncFlowOptions | undefined;
  relationMessageMapping: { [key: number]: RelationMessage };
}

export interface StartFlowInput_RelationMessageMappingEntry {
  key: number;
  value: RelationMessage | undefined;
}

export interface StartNormalizeInput {
  flowConnectionConfigs: FlowConnectionConfigs | undefined;
}

export interface GetLastSyncedIDInput {
  peerConnectionConfig: Peer | undefined;
  flowJobName: string;
}

export interface EnsurePullabilityInput {
  peerConnectionConfig: Peer | undefined;
  flowJobName: string;
  sourceTableIdentifier: string;
}

export interface EnsurePullabilityBatchInput {
  peerConnectionConfig: Peer | undefined;
  flowJobName: string;
  sourceTableIdentifiers: string[];
}

export interface PostgresTableIdentifier {
  relId: number;
}

export interface TableIdentifier {
  postgresTableIdentifier?: PostgresTableIdentifier | undefined;
}

export interface EnsurePullabilityOutput {
  tableIdentifier: TableIdentifier | undefined;
}

export interface EnsurePullabilityBatchOutput {
  tableIdentifierMapping: { [key: string]: TableIdentifier };
}

export interface EnsurePullabilityBatchOutput_TableIdentifierMappingEntry {
  key: string;
  value: TableIdentifier | undefined;
}

export interface SetupReplicationInput {
  peerConnectionConfig: Peer | undefined;
  flowJobName: string;
  tableNameMapping: { [key: string]: string };
  /** replicate to destination using ctid */
  destinationPeer: Peer | undefined;
  doInitialCopy: boolean;
  existingPublicationName: string;
  existingReplicationSlotName: string;
}

export interface SetupReplicationInput_TableNameMappingEntry {
  key: string;
  value: string;
}

export interface SetupReplicationOutput {
  slotName: string;
  snapshotName: string;
}

export interface CreateRawTableInput {
  peerConnectionConfig: Peer | undefined;
  flowJobName: string;
  tableNameMapping: { [key: string]: string };
  cdcSyncMode: QRepSyncMode;
}

export interface CreateRawTableInput_TableNameMappingEntry {
  key: string;
  value: string;
}

export interface CreateRawTableOutput {
  tableIdentifier: string;
}

export interface TableSchema {
  tableIdentifier: string;
  /**
   * list of column names and types, types can be one of the following:
   * "string", "int", "float", "bool", "timestamp".
   */
  columns: { [key: string]: string };
  primaryKeyColumns: string[];
  isReplicaIdentityFull: boolean;
}

export interface TableSchema_ColumnsEntry {
  key: string;
  value: string;
}

export interface GetTableSchemaBatchInput {
  peerConnectionConfig: Peer | undefined;
  tableIdentifiers: string[];
}

export interface GetTableSchemaBatchOutput {
  tableNameSchemaMapping: { [key: string]: TableSchema };
}

export interface GetTableSchemaBatchOutput_TableNameSchemaMappingEntry {
  key: string;
  value: TableSchema | undefined;
}

export interface SetupNormalizedTableInput {
  peerConnectionConfig: Peer | undefined;
  tableIdentifier: string;
  sourceTableSchema: TableSchema | undefined;
}

export interface SetupNormalizedTableBatchInput {
  peerConnectionConfig: Peer | undefined;
  tableNameSchemaMapping: { [key: string]: TableSchema };
  /** migration related columns */
  softDeleteColName: string;
  syncedAtColName: string;
}

export interface SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry {
  key: string;
  value: TableSchema | undefined;
}

export interface SetupNormalizedTableOutput {
  tableIdentifier: string;
  alreadyExists: boolean;
}

export interface SetupNormalizedTableBatchOutput {
  tableExistsMapping: { [key: string]: boolean };
}

export interface SetupNormalizedTableBatchOutput_TableExistsMappingEntry {
  key: string;
  value: boolean;
}

/** partition ranges [start, end] inclusive */
export interface IntPartitionRange {
  start: number;
  end: number;
}

export interface TimestampPartitionRange {
  start: Date | undefined;
  end: Date | undefined;
}

export interface TID {
  blockNumber: number;
  offsetNumber: number;
}

export interface TIDPartitionRange {
  start: TID | undefined;
  end: TID | undefined;
}

export interface PartitionRange {
  intRange?: IntPartitionRange | undefined;
  timestampRange?: TimestampPartitionRange | undefined;
  tidRange?: TIDPartitionRange | undefined;
}

export interface QRepWriteMode {
  writeType: QRepWriteType;
  upsertKeyColumns: string[];
}

export interface QRepConfig {
  flowJobName: string;
  sourcePeer: Peer | undefined;
  destinationPeer: Peer | undefined;
  destinationTableIdentifier: string;
  query: string;
  watermarkTable: string;
  watermarkColumn: string;
  initialCopyOnly: boolean;
  syncMode: QRepSyncMode;
  /** DEPRECATED: eliminate when breaking changes are allowed. */
  batchSizeInt: number;
  /** DEPRECATED: eliminate when breaking changes are allowed. */
  batchDurationSeconds: number;
  maxParallelWorkers: number;
  /** time to wait between getting partitions to process */
  waitBetweenBatchesSeconds: number;
  writeMode:
    | QRepWriteMode
    | undefined;
  /**
   * This is only used when sync_mode is AVRO
   * this is the location where the avro files will be written
   * if this starts with gs:// then it will be written to GCS
   * if this starts with s3:// then it will be written to S3, only supported in Snowflake
   * if nothing is specified then it will be written to local disk
   * if using GCS or S3 make sure your instance has the correct permissions.
   */
  stagingPath: string;
  /**
   * This setting overrides batch_size_int and batch_duration_seconds
   * and instead uses the number of rows per partition to determine
   * how many rows to process per batch.
   */
  numRowsPerPartition: number;
  /** Creates the watermark table on the destination as-is, can be used for some queries. */
  setupWatermarkTableOnDestination: boolean;
  /**
   * create new tables with "_peerdb_resync" suffix, perform initial load and then swap the new table with the old ones
   * to be used after the old mirror is dropped
   */
  dstTableFullResync: boolean;
}

export interface QRepPartition {
  partitionId: string;
  range: PartitionRange | undefined;
  fullTablePartition: boolean;
}

export interface QRepPartitionBatch {
  batchId: number;
  partitions: QRepPartition[];
}

export interface QRepParitionResult {
  partitions: QRepPartition[];
}

export interface DropFlowInput {
  flowName: string;
}

export interface DeltaAddedColumn {
  columnName: string;
  columnType: string;
}

export interface TableSchemaDelta {
  srcTableName: string;
  dstTableName: string;
  addedColumns: DeltaAddedColumn[];
}

export interface ReplayTableSchemaDeltaInput {
  flowConnectionConfigs: FlowConnectionConfigs | undefined;
  tableSchemaDeltas: TableSchemaDelta[];
}

export interface QRepFlowState {
  lastPartition: QRepPartition | undefined;
  numPartitionsProcessed: number;
  needsResync: boolean;
  disableWaitForNewRows: boolean;
}

function createBaseTableNameMapping(): TableNameMapping {
  return { sourceTableName: "", destinationTableName: "" };
}

export const TableNameMapping = {
  encode(message: TableNameMapping, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sourceTableName !== "") {
      writer.uint32(10).string(message.sourceTableName);
    }
    if (message.destinationTableName !== "") {
      writer.uint32(18).string(message.destinationTableName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableNameMapping {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableNameMapping();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.sourceTableName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.destinationTableName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TableNameMapping {
    return {
      sourceTableName: isSet(object.sourceTableName) ? String(object.sourceTableName) : "",
      destinationTableName: isSet(object.destinationTableName) ? String(object.destinationTableName) : "",
    };
  },

  toJSON(message: TableNameMapping): unknown {
    const obj: any = {};
    if (message.sourceTableName !== "") {
      obj.sourceTableName = message.sourceTableName;
    }
    if (message.destinationTableName !== "") {
      obj.destinationTableName = message.destinationTableName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TableNameMapping>, I>>(base?: I): TableNameMapping {
    return TableNameMapping.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TableNameMapping>, I>>(object: I): TableNameMapping {
    const message = createBaseTableNameMapping();
    message.sourceTableName = object.sourceTableName ?? "";
    message.destinationTableName = object.destinationTableName ?? "";
    return message;
  },
};

function createBaseRelationMessageColumn(): RelationMessageColumn {
  return { flags: 0, name: "", dataType: 0 };
}

export const RelationMessageColumn = {
  encode(message: RelationMessageColumn, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flags !== 0) {
      writer.uint32(8).uint32(message.flags);
    }
    if (message.name !== "") {
      writer.uint32(18).string(message.name);
    }
    if (message.dataType !== 0) {
      writer.uint32(24).uint32(message.dataType);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RelationMessageColumn {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRelationMessageColumn();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.flags = reader.uint32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.name = reader.string();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.dataType = reader.uint32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RelationMessageColumn {
    return {
      flags: isSet(object.flags) ? Number(object.flags) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      dataType: isSet(object.dataType) ? Number(object.dataType) : 0,
    };
  },

  toJSON(message: RelationMessageColumn): unknown {
    const obj: any = {};
    if (message.flags !== 0) {
      obj.flags = Math.round(message.flags);
    }
    if (message.name !== "") {
      obj.name = message.name;
    }
    if (message.dataType !== 0) {
      obj.dataType = Math.round(message.dataType);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RelationMessageColumn>, I>>(base?: I): RelationMessageColumn {
    return RelationMessageColumn.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RelationMessageColumn>, I>>(object: I): RelationMessageColumn {
    const message = createBaseRelationMessageColumn();
    message.flags = object.flags ?? 0;
    message.name = object.name ?? "";
    message.dataType = object.dataType ?? 0;
    return message;
  },
};

function createBaseRelationMessage(): RelationMessage {
  return { relationId: 0, relationName: "", columns: [] };
}

export const RelationMessage = {
  encode(message: RelationMessage, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.relationId !== 0) {
      writer.uint32(8).uint32(message.relationId);
    }
    if (message.relationName !== "") {
      writer.uint32(18).string(message.relationName);
    }
    for (const v of message.columns) {
      RelationMessageColumn.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RelationMessage {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRelationMessage();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.relationId = reader.uint32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.relationName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.columns.push(RelationMessageColumn.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RelationMessage {
    return {
      relationId: isSet(object.relationId) ? Number(object.relationId) : 0,
      relationName: isSet(object.relationName) ? String(object.relationName) : "",
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => RelationMessageColumn.fromJSON(e)) : [],
    };
  },

  toJSON(message: RelationMessage): unknown {
    const obj: any = {};
    if (message.relationId !== 0) {
      obj.relationId = Math.round(message.relationId);
    }
    if (message.relationName !== "") {
      obj.relationName = message.relationName;
    }
    if (message.columns?.length) {
      obj.columns = message.columns.map((e) => RelationMessageColumn.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RelationMessage>, I>>(base?: I): RelationMessage {
    return RelationMessage.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RelationMessage>, I>>(object: I): RelationMessage {
    const message = createBaseRelationMessage();
    message.relationId = object.relationId ?? 0;
    message.relationName = object.relationName ?? "";
    message.columns = object.columns?.map((e) => RelationMessageColumn.fromPartial(e)) || [];
    return message;
  },
};

function createBaseTableMapping(): TableMapping {
  return { sourceTableIdentifier: "", destinationTableIdentifier: "", partitionKey: "", exclude: [] };
}

export const TableMapping = {
  encode(message: TableMapping, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sourceTableIdentifier !== "") {
      writer.uint32(10).string(message.sourceTableIdentifier);
    }
    if (message.destinationTableIdentifier !== "") {
      writer.uint32(18).string(message.destinationTableIdentifier);
    }
    if (message.partitionKey !== "") {
      writer.uint32(26).string(message.partitionKey);
    }
    for (const v of message.exclude) {
      writer.uint32(34).string(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableMapping {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableMapping();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.sourceTableIdentifier = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.destinationTableIdentifier = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.partitionKey = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.exclude.push(reader.string());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TableMapping {
    return {
      sourceTableIdentifier: isSet(object.sourceTableIdentifier) ? String(object.sourceTableIdentifier) : "",
      destinationTableIdentifier: isSet(object.destinationTableIdentifier)
        ? String(object.destinationTableIdentifier)
        : "",
      partitionKey: isSet(object.partitionKey) ? String(object.partitionKey) : "",
      exclude: Array.isArray(object?.exclude) ? object.exclude.map((e: any) => String(e)) : [],
    };
  },

  toJSON(message: TableMapping): unknown {
    const obj: any = {};
    if (message.sourceTableIdentifier !== "") {
      obj.sourceTableIdentifier = message.sourceTableIdentifier;
    }
    if (message.destinationTableIdentifier !== "") {
      obj.destinationTableIdentifier = message.destinationTableIdentifier;
    }
    if (message.partitionKey !== "") {
      obj.partitionKey = message.partitionKey;
    }
    if (message.exclude?.length) {
      obj.exclude = message.exclude;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TableMapping>, I>>(base?: I): TableMapping {
    return TableMapping.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TableMapping>, I>>(object: I): TableMapping {
    const message = createBaseTableMapping();
    message.sourceTableIdentifier = object.sourceTableIdentifier ?? "";
    message.destinationTableIdentifier = object.destinationTableIdentifier ?? "";
    message.partitionKey = object.partitionKey ?? "";
    message.exclude = object.exclude?.map((e) => e) || [];
    return message;
  },
};

function createBaseFlowConnectionConfigs(): FlowConnectionConfigs {
  return {
    source: undefined,
    destination: undefined,
    flowJobName: "",
    tableSchema: undefined,
    tableMappings: [],
    srcTableIdNameMapping: {},
    tableNameSchemaMapping: {},
    metadataPeer: undefined,
    maxBatchSize: 0,
    doInitialCopy: false,
    publicationName: "",
    snapshotNumRowsPerPartition: 0,
    snapshotMaxParallelWorkers: 0,
    snapshotNumTablesInParallel: 0,
    snapshotSyncMode: 0,
    cdcSyncMode: 0,
    snapshotStagingPath: "",
    cdcStagingPath: "",
    softDelete: false,
    replicationSlotName: "",
    pushBatchSize: 0,
    pushParallelism: 0,
    resync: false,
    softDeleteColName: "",
    syncedAtColName: "",
  };
}

export const FlowConnectionConfigs = {
  encode(message: FlowConnectionConfigs, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.source !== undefined) {
      Peer.encode(message.source, writer.uint32(10).fork()).ldelim();
    }
    if (message.destination !== undefined) {
      Peer.encode(message.destination, writer.uint32(18).fork()).ldelim();
    }
    if (message.flowJobName !== "") {
      writer.uint32(26).string(message.flowJobName);
    }
    if (message.tableSchema !== undefined) {
      TableSchema.encode(message.tableSchema, writer.uint32(34).fork()).ldelim();
    }
    for (const v of message.tableMappings) {
      TableMapping.encode(v!, writer.uint32(42).fork()).ldelim();
    }
    Object.entries(message.srcTableIdNameMapping).forEach(([key, value]) => {
      FlowConnectionConfigs_SrcTableIdNameMappingEntry.encode({ key: key as any, value }, writer.uint32(50).fork())
        .ldelim();
    });
    Object.entries(message.tableNameSchemaMapping).forEach(([key, value]) => {
      FlowConnectionConfigs_TableNameSchemaMappingEntry.encode({ key: key as any, value }, writer.uint32(58).fork())
        .ldelim();
    });
    if (message.metadataPeer !== undefined) {
      Peer.encode(message.metadataPeer, writer.uint32(66).fork()).ldelim();
    }
    if (message.maxBatchSize !== 0) {
      writer.uint32(72).uint32(message.maxBatchSize);
    }
    if (message.doInitialCopy === true) {
      writer.uint32(80).bool(message.doInitialCopy);
    }
    if (message.publicationName !== "") {
      writer.uint32(90).string(message.publicationName);
    }
    if (message.snapshotNumRowsPerPartition !== 0) {
      writer.uint32(96).uint32(message.snapshotNumRowsPerPartition);
    }
    if (message.snapshotMaxParallelWorkers !== 0) {
      writer.uint32(104).uint32(message.snapshotMaxParallelWorkers);
    }
    if (message.snapshotNumTablesInParallel !== 0) {
      writer.uint32(112).uint32(message.snapshotNumTablesInParallel);
    }
    if (message.snapshotSyncMode !== 0) {
      writer.uint32(120).int32(message.snapshotSyncMode);
    }
    if (message.cdcSyncMode !== 0) {
      writer.uint32(128).int32(message.cdcSyncMode);
    }
    if (message.snapshotStagingPath !== "") {
      writer.uint32(138).string(message.snapshotStagingPath);
    }
    if (message.cdcStagingPath !== "") {
      writer.uint32(146).string(message.cdcStagingPath);
    }
    if (message.softDelete === true) {
      writer.uint32(152).bool(message.softDelete);
    }
    if (message.replicationSlotName !== "") {
      writer.uint32(162).string(message.replicationSlotName);
    }
    if (message.pushBatchSize !== 0) {
      writer.uint32(168).int64(message.pushBatchSize);
    }
    if (message.pushParallelism !== 0) {
      writer.uint32(176).int64(message.pushParallelism);
    }
    if (message.resync === true) {
      writer.uint32(184).bool(message.resync);
    }
    if (message.softDeleteColName !== "") {
      writer.uint32(194).string(message.softDeleteColName);
    }
    if (message.syncedAtColName !== "") {
      writer.uint32(202).string(message.syncedAtColName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FlowConnectionConfigs {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFlowConnectionConfigs();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.source = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.destination = Peer.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.tableSchema = TableSchema.decode(reader, reader.uint32());
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.tableMappings.push(TableMapping.decode(reader, reader.uint32()));
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          const entry6 = FlowConnectionConfigs_SrcTableIdNameMappingEntry.decode(reader, reader.uint32());
          if (entry6.value !== undefined) {
            message.srcTableIdNameMapping[entry6.key] = entry6.value;
          }
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          const entry7 = FlowConnectionConfigs_TableNameSchemaMappingEntry.decode(reader, reader.uint32());
          if (entry7.value !== undefined) {
            message.tableNameSchemaMapping[entry7.key] = entry7.value;
          }
          continue;
        case 8:
          if (tag !== 66) {
            break;
          }

          message.metadataPeer = Peer.decode(reader, reader.uint32());
          continue;
        case 9:
          if (tag !== 72) {
            break;
          }

          message.maxBatchSize = reader.uint32();
          continue;
        case 10:
          if (tag !== 80) {
            break;
          }

          message.doInitialCopy = reader.bool();
          continue;
        case 11:
          if (tag !== 90) {
            break;
          }

          message.publicationName = reader.string();
          continue;
        case 12:
          if (tag !== 96) {
            break;
          }

          message.snapshotNumRowsPerPartition = reader.uint32();
          continue;
        case 13:
          if (tag !== 104) {
            break;
          }

          message.snapshotMaxParallelWorkers = reader.uint32();
          continue;
        case 14:
          if (tag !== 112) {
            break;
          }

          message.snapshotNumTablesInParallel = reader.uint32();
          continue;
        case 15:
          if (tag !== 120) {
            break;
          }

          message.snapshotSyncMode = reader.int32() as any;
          continue;
        case 16:
          if (tag !== 128) {
            break;
          }

          message.cdcSyncMode = reader.int32() as any;
          continue;
        case 17:
          if (tag !== 138) {
            break;
          }

          message.snapshotStagingPath = reader.string();
          continue;
        case 18:
          if (tag !== 146) {
            break;
          }

          message.cdcStagingPath = reader.string();
          continue;
        case 19:
          if (tag !== 152) {
            break;
          }

          message.softDelete = reader.bool();
          continue;
        case 20:
          if (tag !== 162) {
            break;
          }

          message.replicationSlotName = reader.string();
          continue;
        case 21:
          if (tag !== 168) {
            break;
          }

          message.pushBatchSize = longToNumber(reader.int64() as Long);
          continue;
        case 22:
          if (tag !== 176) {
            break;
          }

          message.pushParallelism = longToNumber(reader.int64() as Long);
          continue;
        case 23:
          if (tag !== 184) {
            break;
          }

          message.resync = reader.bool();
          continue;
        case 24:
          if (tag !== 194) {
            break;
          }

          message.softDeleteColName = reader.string();
          continue;
        case 25:
          if (tag !== 202) {
            break;
          }

          message.syncedAtColName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): FlowConnectionConfigs {
    return {
      source: isSet(object.source) ? Peer.fromJSON(object.source) : undefined,
      destination: isSet(object.destination) ? Peer.fromJSON(object.destination) : undefined,
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      tableSchema: isSet(object.tableSchema) ? TableSchema.fromJSON(object.tableSchema) : undefined,
      tableMappings: Array.isArray(object?.tableMappings)
        ? object.tableMappings.map((e: any) => TableMapping.fromJSON(e))
        : [],
      srcTableIdNameMapping: isObject(object.srcTableIdNameMapping)
        ? Object.entries(object.srcTableIdNameMapping).reduce<{ [key: number]: string }>((acc, [key, value]) => {
          acc[Number(key)] = String(value);
          return acc;
        }, {})
        : {},
      tableNameSchemaMapping: isObject(object.tableNameSchemaMapping)
        ? Object.entries(object.tableNameSchemaMapping).reduce<{ [key: string]: TableSchema }>((acc, [key, value]) => {
          acc[key] = TableSchema.fromJSON(value);
          return acc;
        }, {})
        : {},
      metadataPeer: isSet(object.metadataPeer) ? Peer.fromJSON(object.metadataPeer) : undefined,
      maxBatchSize: isSet(object.maxBatchSize) ? Number(object.maxBatchSize) : 0,
      doInitialCopy: isSet(object.doInitialCopy) ? Boolean(object.doInitialCopy) : false,
      publicationName: isSet(object.publicationName) ? String(object.publicationName) : "",
      snapshotNumRowsPerPartition: isSet(object.snapshotNumRowsPerPartition)
        ? Number(object.snapshotNumRowsPerPartition)
        : 0,
      snapshotMaxParallelWorkers: isSet(object.snapshotMaxParallelWorkers)
        ? Number(object.snapshotMaxParallelWorkers)
        : 0,
      snapshotNumTablesInParallel: isSet(object.snapshotNumTablesInParallel)
        ? Number(object.snapshotNumTablesInParallel)
        : 0,
      snapshotSyncMode: isSet(object.snapshotSyncMode) ? qRepSyncModeFromJSON(object.snapshotSyncMode) : 0,
      cdcSyncMode: isSet(object.cdcSyncMode) ? qRepSyncModeFromJSON(object.cdcSyncMode) : 0,
      snapshotStagingPath: isSet(object.snapshotStagingPath) ? String(object.snapshotStagingPath) : "",
      cdcStagingPath: isSet(object.cdcStagingPath) ? String(object.cdcStagingPath) : "",
      softDelete: isSet(object.softDelete) ? Boolean(object.softDelete) : false,
      replicationSlotName: isSet(object.replicationSlotName) ? String(object.replicationSlotName) : "",
      pushBatchSize: isSet(object.pushBatchSize) ? Number(object.pushBatchSize) : 0,
      pushParallelism: isSet(object.pushParallelism) ? Number(object.pushParallelism) : 0,
      resync: isSet(object.resync) ? Boolean(object.resync) : false,
      softDeleteColName: isSet(object.softDeleteColName) ? String(object.softDeleteColName) : "",
      syncedAtColName: isSet(object.syncedAtColName) ? String(object.syncedAtColName) : "",
    };
  },

  toJSON(message: FlowConnectionConfigs): unknown {
    const obj: any = {};
    if (message.source !== undefined) {
      obj.source = Peer.toJSON(message.source);
    }
    if (message.destination !== undefined) {
      obj.destination = Peer.toJSON(message.destination);
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.tableSchema !== undefined) {
      obj.tableSchema = TableSchema.toJSON(message.tableSchema);
    }
    if (message.tableMappings?.length) {
      obj.tableMappings = message.tableMappings.map((e) => TableMapping.toJSON(e));
    }
    if (message.srcTableIdNameMapping) {
      const entries = Object.entries(message.srcTableIdNameMapping);
      if (entries.length > 0) {
        obj.srcTableIdNameMapping = {};
        entries.forEach(([k, v]) => {
          obj.srcTableIdNameMapping[k] = v;
        });
      }
    }
    if (message.tableNameSchemaMapping) {
      const entries = Object.entries(message.tableNameSchemaMapping);
      if (entries.length > 0) {
        obj.tableNameSchemaMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableNameSchemaMapping[k] = TableSchema.toJSON(v);
        });
      }
    }
    if (message.metadataPeer !== undefined) {
      obj.metadataPeer = Peer.toJSON(message.metadataPeer);
    }
    if (message.maxBatchSize !== 0) {
      obj.maxBatchSize = Math.round(message.maxBatchSize);
    }
    if (message.doInitialCopy === true) {
      obj.doInitialCopy = message.doInitialCopy;
    }
    if (message.publicationName !== "") {
      obj.publicationName = message.publicationName;
    }
    if (message.snapshotNumRowsPerPartition !== 0) {
      obj.snapshotNumRowsPerPartition = Math.round(message.snapshotNumRowsPerPartition);
    }
    if (message.snapshotMaxParallelWorkers !== 0) {
      obj.snapshotMaxParallelWorkers = Math.round(message.snapshotMaxParallelWorkers);
    }
    if (message.snapshotNumTablesInParallel !== 0) {
      obj.snapshotNumTablesInParallel = Math.round(message.snapshotNumTablesInParallel);
    }
    if (message.snapshotSyncMode !== 0) {
      obj.snapshotSyncMode = qRepSyncModeToJSON(message.snapshotSyncMode);
    }
    if (message.cdcSyncMode !== 0) {
      obj.cdcSyncMode = qRepSyncModeToJSON(message.cdcSyncMode);
    }
    if (message.snapshotStagingPath !== "") {
      obj.snapshotStagingPath = message.snapshotStagingPath;
    }
    if (message.cdcStagingPath !== "") {
      obj.cdcStagingPath = message.cdcStagingPath;
    }
    if (message.softDelete === true) {
      obj.softDelete = message.softDelete;
    }
    if (message.replicationSlotName !== "") {
      obj.replicationSlotName = message.replicationSlotName;
    }
    if (message.pushBatchSize !== 0) {
      obj.pushBatchSize = Math.round(message.pushBatchSize);
    }
    if (message.pushParallelism !== 0) {
      obj.pushParallelism = Math.round(message.pushParallelism);
    }
    if (message.resync === true) {
      obj.resync = message.resync;
    }
    if (message.softDeleteColName !== "") {
      obj.softDeleteColName = message.softDeleteColName;
    }
    if (message.syncedAtColName !== "") {
      obj.syncedAtColName = message.syncedAtColName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<FlowConnectionConfigs>, I>>(base?: I): FlowConnectionConfigs {
    return FlowConnectionConfigs.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<FlowConnectionConfigs>, I>>(object: I): FlowConnectionConfigs {
    const message = createBaseFlowConnectionConfigs();
    message.source = (object.source !== undefined && object.source !== null)
      ? Peer.fromPartial(object.source)
      : undefined;
    message.destination = (object.destination !== undefined && object.destination !== null)
      ? Peer.fromPartial(object.destination)
      : undefined;
    message.flowJobName = object.flowJobName ?? "";
    message.tableSchema = (object.tableSchema !== undefined && object.tableSchema !== null)
      ? TableSchema.fromPartial(object.tableSchema)
      : undefined;
    message.tableMappings = object.tableMappings?.map((e) => TableMapping.fromPartial(e)) || [];
    message.srcTableIdNameMapping = Object.entries(object.srcTableIdNameMapping ?? {}).reduce<
      { [key: number]: string }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = String(value);
      }
      return acc;
    }, {});
    message.tableNameSchemaMapping = Object.entries(object.tableNameSchemaMapping ?? {}).reduce<
      { [key: string]: TableSchema }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[key] = TableSchema.fromPartial(value);
      }
      return acc;
    }, {});
    message.metadataPeer = (object.metadataPeer !== undefined && object.metadataPeer !== null)
      ? Peer.fromPartial(object.metadataPeer)
      : undefined;
    message.maxBatchSize = object.maxBatchSize ?? 0;
    message.doInitialCopy = object.doInitialCopy ?? false;
    message.publicationName = object.publicationName ?? "";
    message.snapshotNumRowsPerPartition = object.snapshotNumRowsPerPartition ?? 0;
    message.snapshotMaxParallelWorkers = object.snapshotMaxParallelWorkers ?? 0;
    message.snapshotNumTablesInParallel = object.snapshotNumTablesInParallel ?? 0;
    message.snapshotSyncMode = object.snapshotSyncMode ?? 0;
    message.cdcSyncMode = object.cdcSyncMode ?? 0;
    message.snapshotStagingPath = object.snapshotStagingPath ?? "";
    message.cdcStagingPath = object.cdcStagingPath ?? "";
    message.softDelete = object.softDelete ?? false;
    message.replicationSlotName = object.replicationSlotName ?? "";
    message.pushBatchSize = object.pushBatchSize ?? 0;
    message.pushParallelism = object.pushParallelism ?? 0;
    message.resync = object.resync ?? false;
    message.softDeleteColName = object.softDeleteColName ?? "";
    message.syncedAtColName = object.syncedAtColName ?? "";
    return message;
  },
};

function createBaseFlowConnectionConfigs_SrcTableIdNameMappingEntry(): FlowConnectionConfigs_SrcTableIdNameMappingEntry {
  return { key: 0, value: "" };
}

export const FlowConnectionConfigs_SrcTableIdNameMappingEntry = {
  encode(
    message: FlowConnectionConfigs_SrcTableIdNameMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FlowConnectionConfigs_SrcTableIdNameMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFlowConnectionConfigs_SrcTableIdNameMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.key = reader.uint32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): FlowConnectionConfigs_SrcTableIdNameMappingEntry {
    return { key: isSet(object.key) ? Number(object.key) : 0, value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: FlowConnectionConfigs_SrcTableIdNameMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== 0) {
      obj.key = Math.round(message.key);
    }
    if (message.value !== "") {
      obj.value = message.value;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<FlowConnectionConfigs_SrcTableIdNameMappingEntry>, I>>(
    base?: I,
  ): FlowConnectionConfigs_SrcTableIdNameMappingEntry {
    return FlowConnectionConfigs_SrcTableIdNameMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<FlowConnectionConfigs_SrcTableIdNameMappingEntry>, I>>(
    object: I,
  ): FlowConnectionConfigs_SrcTableIdNameMappingEntry {
    const message = createBaseFlowConnectionConfigs_SrcTableIdNameMappingEntry();
    message.key = object.key ?? 0;
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseFlowConnectionConfigs_TableNameSchemaMappingEntry(): FlowConnectionConfigs_TableNameSchemaMappingEntry {
  return { key: "", value: undefined };
}

export const FlowConnectionConfigs_TableNameSchemaMappingEntry = {
  encode(
    message: FlowConnectionConfigs_TableNameSchemaMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== undefined) {
      TableSchema.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FlowConnectionConfigs_TableNameSchemaMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFlowConnectionConfigs_TableNameSchemaMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = TableSchema.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): FlowConnectionConfigs_TableNameSchemaMappingEntry {
    return {
      key: isSet(object.key) ? String(object.key) : "",
      value: isSet(object.value) ? TableSchema.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: FlowConnectionConfigs_TableNameSchemaMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== undefined) {
      obj.value = TableSchema.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<FlowConnectionConfigs_TableNameSchemaMappingEntry>, I>>(
    base?: I,
  ): FlowConnectionConfigs_TableNameSchemaMappingEntry {
    return FlowConnectionConfigs_TableNameSchemaMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<FlowConnectionConfigs_TableNameSchemaMappingEntry>, I>>(
    object: I,
  ): FlowConnectionConfigs_TableNameSchemaMappingEntry {
    const message = createBaseFlowConnectionConfigs_TableNameSchemaMappingEntry();
    message.key = object.key ?? "";
    message.value = (object.value !== undefined && object.value !== null)
      ? TableSchema.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseRenameTableOption(): RenameTableOption {
  return { currentName: "", newName: "", tableSchema: undefined };
}

export const RenameTableOption = {
  encode(message: RenameTableOption, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.currentName !== "") {
      writer.uint32(10).string(message.currentName);
    }
    if (message.newName !== "") {
      writer.uint32(18).string(message.newName);
    }
    if (message.tableSchema !== undefined) {
      TableSchema.encode(message.tableSchema, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RenameTableOption {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRenameTableOption();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.currentName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.newName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.tableSchema = TableSchema.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RenameTableOption {
    return {
      currentName: isSet(object.currentName) ? String(object.currentName) : "",
      newName: isSet(object.newName) ? String(object.newName) : "",
      tableSchema: isSet(object.tableSchema) ? TableSchema.fromJSON(object.tableSchema) : undefined,
    };
  },

  toJSON(message: RenameTableOption): unknown {
    const obj: any = {};
    if (message.currentName !== "") {
      obj.currentName = message.currentName;
    }
    if (message.newName !== "") {
      obj.newName = message.newName;
    }
    if (message.tableSchema !== undefined) {
      obj.tableSchema = TableSchema.toJSON(message.tableSchema);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RenameTableOption>, I>>(base?: I): RenameTableOption {
    return RenameTableOption.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RenameTableOption>, I>>(object: I): RenameTableOption {
    const message = createBaseRenameTableOption();
    message.currentName = object.currentName ?? "";
    message.newName = object.newName ?? "";
    message.tableSchema = (object.tableSchema !== undefined && object.tableSchema !== null)
      ? TableSchema.fromPartial(object.tableSchema)
      : undefined;
    return message;
  },
};

function createBaseRenameTablesInput(): RenameTablesInput {
  return {
    flowJobName: "",
    peer: undefined,
    renameTableOptions: [],
    softDeleteColName: undefined,
    syncedAtColName: undefined,
  };
}

export const RenameTablesInput = {
  encode(message: RenameTablesInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(10).string(message.flowJobName);
    }
    if (message.peer !== undefined) {
      Peer.encode(message.peer, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.renameTableOptions) {
      RenameTableOption.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    if (message.softDeleteColName !== undefined) {
      writer.uint32(34).string(message.softDeleteColName);
    }
    if (message.syncedAtColName !== undefined) {
      writer.uint32(42).string(message.syncedAtColName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RenameTablesInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRenameTablesInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.peer = Peer.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.renameTableOptions.push(RenameTableOption.decode(reader, reader.uint32()));
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.softDeleteColName = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.syncedAtColName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RenameTablesInput {
    return {
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      peer: isSet(object.peer) ? Peer.fromJSON(object.peer) : undefined,
      renameTableOptions: Array.isArray(object?.renameTableOptions)
        ? object.renameTableOptions.map((e: any) => RenameTableOption.fromJSON(e))
        : [],
      softDeleteColName: isSet(object.softDeleteColName) ? String(object.softDeleteColName) : undefined,
      syncedAtColName: isSet(object.syncedAtColName) ? String(object.syncedAtColName) : undefined,
    };
  },

  toJSON(message: RenameTablesInput): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.peer !== undefined) {
      obj.peer = Peer.toJSON(message.peer);
    }
    if (message.renameTableOptions?.length) {
      obj.renameTableOptions = message.renameTableOptions.map((e) => RenameTableOption.toJSON(e));
    }
    if (message.softDeleteColName !== undefined) {
      obj.softDeleteColName = message.softDeleteColName;
    }
    if (message.syncedAtColName !== undefined) {
      obj.syncedAtColName = message.syncedAtColName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RenameTablesInput>, I>>(base?: I): RenameTablesInput {
    return RenameTablesInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RenameTablesInput>, I>>(object: I): RenameTablesInput {
    const message = createBaseRenameTablesInput();
    message.flowJobName = object.flowJobName ?? "";
    message.peer = (object.peer !== undefined && object.peer !== null) ? Peer.fromPartial(object.peer) : undefined;
    message.renameTableOptions = object.renameTableOptions?.map((e) => RenameTableOption.fromPartial(e)) || [];
    message.softDeleteColName = object.softDeleteColName ?? undefined;
    message.syncedAtColName = object.syncedAtColName ?? undefined;
    return message;
  },
};

function createBaseRenameTablesOutput(): RenameTablesOutput {
  return { flowJobName: "" };
}

export const RenameTablesOutput = {
  encode(message: RenameTablesOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(10).string(message.flowJobName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RenameTablesOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRenameTablesOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): RenameTablesOutput {
    return { flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "" };
  },

  toJSON(message: RenameTablesOutput): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<RenameTablesOutput>, I>>(base?: I): RenameTablesOutput {
    return RenameTablesOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RenameTablesOutput>, I>>(object: I): RenameTablesOutput {
    const message = createBaseRenameTablesOutput();
    message.flowJobName = object.flowJobName ?? "";
    return message;
  },
};

function createBaseCreateTablesFromExistingInput(): CreateTablesFromExistingInput {
  return { flowJobName: "", peer: undefined, newToExistingTableMapping: {} };
}

export const CreateTablesFromExistingInput = {
  encode(message: CreateTablesFromExistingInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(10).string(message.flowJobName);
    }
    if (message.peer !== undefined) {
      Peer.encode(message.peer, writer.uint32(18).fork()).ldelim();
    }
    Object.entries(message.newToExistingTableMapping).forEach(([key, value]) => {
      CreateTablesFromExistingInput_NewToExistingTableMappingEntry.encode(
        { key: key as any, value },
        writer.uint32(26).fork(),
      ).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateTablesFromExistingInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateTablesFromExistingInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.peer = Peer.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          const entry3 = CreateTablesFromExistingInput_NewToExistingTableMappingEntry.decode(reader, reader.uint32());
          if (entry3.value !== undefined) {
            message.newToExistingTableMapping[entry3.key] = entry3.value;
          }
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateTablesFromExistingInput {
    return {
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      peer: isSet(object.peer) ? Peer.fromJSON(object.peer) : undefined,
      newToExistingTableMapping: isObject(object.newToExistingTableMapping)
        ? Object.entries(object.newToExistingTableMapping).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: CreateTablesFromExistingInput): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.peer !== undefined) {
      obj.peer = Peer.toJSON(message.peer);
    }
    if (message.newToExistingTableMapping) {
      const entries = Object.entries(message.newToExistingTableMapping);
      if (entries.length > 0) {
        obj.newToExistingTableMapping = {};
        entries.forEach(([k, v]) => {
          obj.newToExistingTableMapping[k] = v;
        });
      }
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateTablesFromExistingInput>, I>>(base?: I): CreateTablesFromExistingInput {
    return CreateTablesFromExistingInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateTablesFromExistingInput>, I>>(
    object: I,
  ): CreateTablesFromExistingInput {
    const message = createBaseCreateTablesFromExistingInput();
    message.flowJobName = object.flowJobName ?? "";
    message.peer = (object.peer !== undefined && object.peer !== null) ? Peer.fromPartial(object.peer) : undefined;
    message.newToExistingTableMapping = Object.entries(object.newToExistingTableMapping ?? {}).reduce<
      { [key: string]: string }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[key] = String(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseCreateTablesFromExistingInput_NewToExistingTableMappingEntry(): CreateTablesFromExistingInput_NewToExistingTableMappingEntry {
  return { key: "", value: "" };
}

export const CreateTablesFromExistingInput_NewToExistingTableMappingEntry = {
  encode(
    message: CreateTablesFromExistingInput_NewToExistingTableMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(
    input: _m0.Reader | Uint8Array,
    length?: number,
  ): CreateTablesFromExistingInput_NewToExistingTableMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateTablesFromExistingInput_NewToExistingTableMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateTablesFromExistingInput_NewToExistingTableMappingEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: CreateTablesFromExistingInput_NewToExistingTableMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== "") {
      obj.value = message.value;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateTablesFromExistingInput_NewToExistingTableMappingEntry>, I>>(
    base?: I,
  ): CreateTablesFromExistingInput_NewToExistingTableMappingEntry {
    return CreateTablesFromExistingInput_NewToExistingTableMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateTablesFromExistingInput_NewToExistingTableMappingEntry>, I>>(
    object: I,
  ): CreateTablesFromExistingInput_NewToExistingTableMappingEntry {
    const message = createBaseCreateTablesFromExistingInput_NewToExistingTableMappingEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseCreateTablesFromExistingOutput(): CreateTablesFromExistingOutput {
  return { flowJobName: "" };
}

export const CreateTablesFromExistingOutput = {
  encode(message: CreateTablesFromExistingOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateTablesFromExistingOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateTablesFromExistingOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateTablesFromExistingOutput {
    return { flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "" };
  },

  toJSON(message: CreateTablesFromExistingOutput): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateTablesFromExistingOutput>, I>>(base?: I): CreateTablesFromExistingOutput {
    return CreateTablesFromExistingOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateTablesFromExistingOutput>, I>>(
    object: I,
  ): CreateTablesFromExistingOutput {
    const message = createBaseCreateTablesFromExistingOutput();
    message.flowJobName = object.flowJobName ?? "";
    return message;
  },
};

function createBaseSyncFlowOptions(): SyncFlowOptions {
  return { batchSize: 0, relationMessageMapping: {} };
}

export const SyncFlowOptions = {
  encode(message: SyncFlowOptions, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.batchSize !== 0) {
      writer.uint32(8).int32(message.batchSize);
    }
    Object.entries(message.relationMessageMapping).forEach(([key, value]) => {
      SyncFlowOptions_RelationMessageMappingEntry.encode({ key: key as any, value }, writer.uint32(18).fork()).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SyncFlowOptions {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSyncFlowOptions();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.batchSize = reader.int32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          const entry2 = SyncFlowOptions_RelationMessageMappingEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.relationMessageMapping[entry2.key] = entry2.value;
          }
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SyncFlowOptions {
    return {
      batchSize: isSet(object.batchSize) ? Number(object.batchSize) : 0,
      relationMessageMapping: isObject(object.relationMessageMapping)
        ? Object.entries(object.relationMessageMapping).reduce<{ [key: number]: RelationMessage }>(
          (acc, [key, value]) => {
            acc[Number(key)] = RelationMessage.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
    };
  },

  toJSON(message: SyncFlowOptions): unknown {
    const obj: any = {};
    if (message.batchSize !== 0) {
      obj.batchSize = Math.round(message.batchSize);
    }
    if (message.relationMessageMapping) {
      const entries = Object.entries(message.relationMessageMapping);
      if (entries.length > 0) {
        obj.relationMessageMapping = {};
        entries.forEach(([k, v]) => {
          obj.relationMessageMapping[k] = RelationMessage.toJSON(v);
        });
      }
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SyncFlowOptions>, I>>(base?: I): SyncFlowOptions {
    return SyncFlowOptions.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SyncFlowOptions>, I>>(object: I): SyncFlowOptions {
    const message = createBaseSyncFlowOptions();
    message.batchSize = object.batchSize ?? 0;
    message.relationMessageMapping = Object.entries(object.relationMessageMapping ?? {}).reduce<
      { [key: number]: RelationMessage }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = RelationMessage.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseSyncFlowOptions_RelationMessageMappingEntry(): SyncFlowOptions_RelationMessageMappingEntry {
  return { key: 0, value: undefined };
}

export const SyncFlowOptions_RelationMessageMappingEntry = {
  encode(message: SyncFlowOptions_RelationMessageMappingEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      RelationMessage.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SyncFlowOptions_RelationMessageMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSyncFlowOptions_RelationMessageMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.key = reader.uint32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = RelationMessage.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SyncFlowOptions_RelationMessageMappingEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? RelationMessage.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: SyncFlowOptions_RelationMessageMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== 0) {
      obj.key = Math.round(message.key);
    }
    if (message.value !== undefined) {
      obj.value = RelationMessage.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SyncFlowOptions_RelationMessageMappingEntry>, I>>(
    base?: I,
  ): SyncFlowOptions_RelationMessageMappingEntry {
    return SyncFlowOptions_RelationMessageMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SyncFlowOptions_RelationMessageMappingEntry>, I>>(
    object: I,
  ): SyncFlowOptions_RelationMessageMappingEntry {
    const message = createBaseSyncFlowOptions_RelationMessageMappingEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? RelationMessage.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseNormalizeFlowOptions(): NormalizeFlowOptions {
  return { batchSize: 0 };
}

export const NormalizeFlowOptions = {
  encode(message: NormalizeFlowOptions, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.batchSize !== 0) {
      writer.uint32(8).int32(message.batchSize);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): NormalizeFlowOptions {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseNormalizeFlowOptions();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.batchSize = reader.int32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): NormalizeFlowOptions {
    return { batchSize: isSet(object.batchSize) ? Number(object.batchSize) : 0 };
  },

  toJSON(message: NormalizeFlowOptions): unknown {
    const obj: any = {};
    if (message.batchSize !== 0) {
      obj.batchSize = Math.round(message.batchSize);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<NormalizeFlowOptions>, I>>(base?: I): NormalizeFlowOptions {
    return NormalizeFlowOptions.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<NormalizeFlowOptions>, I>>(object: I): NormalizeFlowOptions {
    const message = createBaseNormalizeFlowOptions();
    message.batchSize = object.batchSize ?? 0;
    return message;
  },
};

function createBaseLastSyncState(): LastSyncState {
  return { checkpoint: 0, lastSyncedAt: undefined };
}

export const LastSyncState = {
  encode(message: LastSyncState, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.checkpoint !== 0) {
      writer.uint32(8).int64(message.checkpoint);
    }
    if (message.lastSyncedAt !== undefined) {
      Timestamp.encode(toTimestamp(message.lastSyncedAt), writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LastSyncState {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLastSyncState();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.checkpoint = longToNumber(reader.int64() as Long);
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.lastSyncedAt = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): LastSyncState {
    return {
      checkpoint: isSet(object.checkpoint) ? Number(object.checkpoint) : 0,
      lastSyncedAt: isSet(object.lastSyncedAt) ? fromJsonTimestamp(object.lastSyncedAt) : undefined,
    };
  },

  toJSON(message: LastSyncState): unknown {
    const obj: any = {};
    if (message.checkpoint !== 0) {
      obj.checkpoint = Math.round(message.checkpoint);
    }
    if (message.lastSyncedAt !== undefined) {
      obj.lastSyncedAt = message.lastSyncedAt.toISOString();
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<LastSyncState>, I>>(base?: I): LastSyncState {
    return LastSyncState.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<LastSyncState>, I>>(object: I): LastSyncState {
    const message = createBaseLastSyncState();
    message.checkpoint = object.checkpoint ?? 0;
    message.lastSyncedAt = object.lastSyncedAt ?? undefined;
    return message;
  },
};

function createBaseStartFlowInput(): StartFlowInput {
  return {
    lastSyncState: undefined,
    flowConnectionConfigs: undefined,
    syncFlowOptions: undefined,
    relationMessageMapping: {},
  };
}

export const StartFlowInput = {
  encode(message: StartFlowInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.lastSyncState !== undefined) {
      LastSyncState.encode(message.lastSyncState, writer.uint32(10).fork()).ldelim();
    }
    if (message.flowConnectionConfigs !== undefined) {
      FlowConnectionConfigs.encode(message.flowConnectionConfigs, writer.uint32(18).fork()).ldelim();
    }
    if (message.syncFlowOptions !== undefined) {
      SyncFlowOptions.encode(message.syncFlowOptions, writer.uint32(26).fork()).ldelim();
    }
    Object.entries(message.relationMessageMapping).forEach(([key, value]) => {
      StartFlowInput_RelationMessageMappingEntry.encode({ key: key as any, value }, writer.uint32(34).fork()).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StartFlowInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStartFlowInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.lastSyncState = LastSyncState.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowConnectionConfigs = FlowConnectionConfigs.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.syncFlowOptions = SyncFlowOptions.decode(reader, reader.uint32());
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          const entry4 = StartFlowInput_RelationMessageMappingEntry.decode(reader, reader.uint32());
          if (entry4.value !== undefined) {
            message.relationMessageMapping[entry4.key] = entry4.value;
          }
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): StartFlowInput {
    return {
      lastSyncState: isSet(object.lastSyncState) ? LastSyncState.fromJSON(object.lastSyncState) : undefined,
      flowConnectionConfigs: isSet(object.flowConnectionConfigs)
        ? FlowConnectionConfigs.fromJSON(object.flowConnectionConfigs)
        : undefined,
      syncFlowOptions: isSet(object.syncFlowOptions) ? SyncFlowOptions.fromJSON(object.syncFlowOptions) : undefined,
      relationMessageMapping: isObject(object.relationMessageMapping)
        ? Object.entries(object.relationMessageMapping).reduce<{ [key: number]: RelationMessage }>(
          (acc, [key, value]) => {
            acc[Number(key)] = RelationMessage.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
    };
  },

  toJSON(message: StartFlowInput): unknown {
    const obj: any = {};
    if (message.lastSyncState !== undefined) {
      obj.lastSyncState = LastSyncState.toJSON(message.lastSyncState);
    }
    if (message.flowConnectionConfigs !== undefined) {
      obj.flowConnectionConfigs = FlowConnectionConfigs.toJSON(message.flowConnectionConfigs);
    }
    if (message.syncFlowOptions !== undefined) {
      obj.syncFlowOptions = SyncFlowOptions.toJSON(message.syncFlowOptions);
    }
    if (message.relationMessageMapping) {
      const entries = Object.entries(message.relationMessageMapping);
      if (entries.length > 0) {
        obj.relationMessageMapping = {};
        entries.forEach(([k, v]) => {
          obj.relationMessageMapping[k] = RelationMessage.toJSON(v);
        });
      }
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<StartFlowInput>, I>>(base?: I): StartFlowInput {
    return StartFlowInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<StartFlowInput>, I>>(object: I): StartFlowInput {
    const message = createBaseStartFlowInput();
    message.lastSyncState = (object.lastSyncState !== undefined && object.lastSyncState !== null)
      ? LastSyncState.fromPartial(object.lastSyncState)
      : undefined;
    message.flowConnectionConfigs =
      (object.flowConnectionConfigs !== undefined && object.flowConnectionConfigs !== null)
        ? FlowConnectionConfigs.fromPartial(object.flowConnectionConfigs)
        : undefined;
    message.syncFlowOptions = (object.syncFlowOptions !== undefined && object.syncFlowOptions !== null)
      ? SyncFlowOptions.fromPartial(object.syncFlowOptions)
      : undefined;
    message.relationMessageMapping = Object.entries(object.relationMessageMapping ?? {}).reduce<
      { [key: number]: RelationMessage }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = RelationMessage.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseStartFlowInput_RelationMessageMappingEntry(): StartFlowInput_RelationMessageMappingEntry {
  return { key: 0, value: undefined };
}

export const StartFlowInput_RelationMessageMappingEntry = {
  encode(message: StartFlowInput_RelationMessageMappingEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      RelationMessage.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StartFlowInput_RelationMessageMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStartFlowInput_RelationMessageMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.key = reader.uint32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = RelationMessage.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): StartFlowInput_RelationMessageMappingEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? RelationMessage.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: StartFlowInput_RelationMessageMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== 0) {
      obj.key = Math.round(message.key);
    }
    if (message.value !== undefined) {
      obj.value = RelationMessage.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<StartFlowInput_RelationMessageMappingEntry>, I>>(
    base?: I,
  ): StartFlowInput_RelationMessageMappingEntry {
    return StartFlowInput_RelationMessageMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<StartFlowInput_RelationMessageMappingEntry>, I>>(
    object: I,
  ): StartFlowInput_RelationMessageMappingEntry {
    const message = createBaseStartFlowInput_RelationMessageMappingEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? RelationMessage.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseStartNormalizeInput(): StartNormalizeInput {
  return { flowConnectionConfigs: undefined };
}

export const StartNormalizeInput = {
  encode(message: StartNormalizeInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowConnectionConfigs !== undefined) {
      FlowConnectionConfigs.encode(message.flowConnectionConfigs, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StartNormalizeInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStartNormalizeInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowConnectionConfigs = FlowConnectionConfigs.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): StartNormalizeInput {
    return {
      flowConnectionConfigs: isSet(object.flowConnectionConfigs)
        ? FlowConnectionConfigs.fromJSON(object.flowConnectionConfigs)
        : undefined,
    };
  },

  toJSON(message: StartNormalizeInput): unknown {
    const obj: any = {};
    if (message.flowConnectionConfigs !== undefined) {
      obj.flowConnectionConfigs = FlowConnectionConfigs.toJSON(message.flowConnectionConfigs);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<StartNormalizeInput>, I>>(base?: I): StartNormalizeInput {
    return StartNormalizeInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<StartNormalizeInput>, I>>(object: I): StartNormalizeInput {
    const message = createBaseStartNormalizeInput();
    message.flowConnectionConfigs =
      (object.flowConnectionConfigs !== undefined && object.flowConnectionConfigs !== null)
        ? FlowConnectionConfigs.fromPartial(object.flowConnectionConfigs)
        : undefined;
    return message;
  },
};

function createBaseGetLastSyncedIDInput(): GetLastSyncedIDInput {
  return { peerConnectionConfig: undefined, flowJobName: "" };
}

export const GetLastSyncedIDInput = {
  encode(message: GetLastSyncedIDInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetLastSyncedIDInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetLastSyncedIDInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): GetLastSyncedIDInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
    };
  },

  toJSON(message: GetLastSyncedIDInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<GetLastSyncedIDInput>, I>>(base?: I): GetLastSyncedIDInput {
    return GetLastSyncedIDInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<GetLastSyncedIDInput>, I>>(object: I): GetLastSyncedIDInput {
    const message = createBaseGetLastSyncedIDInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.flowJobName = object.flowJobName ?? "";
    return message;
  },
};

function createBaseEnsurePullabilityInput(): EnsurePullabilityInput {
  return { peerConnectionConfig: undefined, flowJobName: "", sourceTableIdentifier: "" };
}

export const EnsurePullabilityInput = {
  encode(message: EnsurePullabilityInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    if (message.sourceTableIdentifier !== "") {
      writer.uint32(26).string(message.sourceTableIdentifier);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EnsurePullabilityInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEnsurePullabilityInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.sourceTableIdentifier = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EnsurePullabilityInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      sourceTableIdentifier: isSet(object.sourceTableIdentifier) ? String(object.sourceTableIdentifier) : "",
    };
  },

  toJSON(message: EnsurePullabilityInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.sourceTableIdentifier !== "") {
      obj.sourceTableIdentifier = message.sourceTableIdentifier;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EnsurePullabilityInput>, I>>(base?: I): EnsurePullabilityInput {
    return EnsurePullabilityInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EnsurePullabilityInput>, I>>(object: I): EnsurePullabilityInput {
    const message = createBaseEnsurePullabilityInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.flowJobName = object.flowJobName ?? "";
    message.sourceTableIdentifier = object.sourceTableIdentifier ?? "";
    return message;
  },
};

function createBaseEnsurePullabilityBatchInput(): EnsurePullabilityBatchInput {
  return { peerConnectionConfig: undefined, flowJobName: "", sourceTableIdentifiers: [] };
}

export const EnsurePullabilityBatchInput = {
  encode(message: EnsurePullabilityBatchInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    for (const v of message.sourceTableIdentifiers) {
      writer.uint32(26).string(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EnsurePullabilityBatchInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEnsurePullabilityBatchInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.sourceTableIdentifiers.push(reader.string());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EnsurePullabilityBatchInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      sourceTableIdentifiers: Array.isArray(object?.sourceTableIdentifiers)
        ? object.sourceTableIdentifiers.map((e: any) => String(e))
        : [],
    };
  },

  toJSON(message: EnsurePullabilityBatchInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.sourceTableIdentifiers?.length) {
      obj.sourceTableIdentifiers = message.sourceTableIdentifiers;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EnsurePullabilityBatchInput>, I>>(base?: I): EnsurePullabilityBatchInput {
    return EnsurePullabilityBatchInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EnsurePullabilityBatchInput>, I>>(object: I): EnsurePullabilityBatchInput {
    const message = createBaseEnsurePullabilityBatchInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.flowJobName = object.flowJobName ?? "";
    message.sourceTableIdentifiers = object.sourceTableIdentifiers?.map((e) => e) || [];
    return message;
  },
};

function createBasePostgresTableIdentifier(): PostgresTableIdentifier {
  return { relId: 0 };
}

export const PostgresTableIdentifier = {
  encode(message: PostgresTableIdentifier, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.relId !== 0) {
      writer.uint32(8).uint32(message.relId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PostgresTableIdentifier {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePostgresTableIdentifier();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.relId = reader.uint32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PostgresTableIdentifier {
    return { relId: isSet(object.relId) ? Number(object.relId) : 0 };
  },

  toJSON(message: PostgresTableIdentifier): unknown {
    const obj: any = {};
    if (message.relId !== 0) {
      obj.relId = Math.round(message.relId);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PostgresTableIdentifier>, I>>(base?: I): PostgresTableIdentifier {
    return PostgresTableIdentifier.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PostgresTableIdentifier>, I>>(object: I): PostgresTableIdentifier {
    const message = createBasePostgresTableIdentifier();
    message.relId = object.relId ?? 0;
    return message;
  },
};

function createBaseTableIdentifier(): TableIdentifier {
  return { postgresTableIdentifier: undefined };
}

export const TableIdentifier = {
  encode(message: TableIdentifier, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.postgresTableIdentifier !== undefined) {
      PostgresTableIdentifier.encode(message.postgresTableIdentifier, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableIdentifier {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableIdentifier();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.postgresTableIdentifier = PostgresTableIdentifier.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TableIdentifier {
    return {
      postgresTableIdentifier: isSet(object.postgresTableIdentifier)
        ? PostgresTableIdentifier.fromJSON(object.postgresTableIdentifier)
        : undefined,
    };
  },

  toJSON(message: TableIdentifier): unknown {
    const obj: any = {};
    if (message.postgresTableIdentifier !== undefined) {
      obj.postgresTableIdentifier = PostgresTableIdentifier.toJSON(message.postgresTableIdentifier);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TableIdentifier>, I>>(base?: I): TableIdentifier {
    return TableIdentifier.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TableIdentifier>, I>>(object: I): TableIdentifier {
    const message = createBaseTableIdentifier();
    message.postgresTableIdentifier =
      (object.postgresTableIdentifier !== undefined && object.postgresTableIdentifier !== null)
        ? PostgresTableIdentifier.fromPartial(object.postgresTableIdentifier)
        : undefined;
    return message;
  },
};

function createBaseEnsurePullabilityOutput(): EnsurePullabilityOutput {
  return { tableIdentifier: undefined };
}

export const EnsurePullabilityOutput = {
  encode(message: EnsurePullabilityOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableIdentifier !== undefined) {
      TableIdentifier.encode(message.tableIdentifier, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EnsurePullabilityOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEnsurePullabilityOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.tableIdentifier = TableIdentifier.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EnsurePullabilityOutput {
    return {
      tableIdentifier: isSet(object.tableIdentifier) ? TableIdentifier.fromJSON(object.tableIdentifier) : undefined,
    };
  },

  toJSON(message: EnsurePullabilityOutput): unknown {
    const obj: any = {};
    if (message.tableIdentifier !== undefined) {
      obj.tableIdentifier = TableIdentifier.toJSON(message.tableIdentifier);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EnsurePullabilityOutput>, I>>(base?: I): EnsurePullabilityOutput {
    return EnsurePullabilityOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EnsurePullabilityOutput>, I>>(object: I): EnsurePullabilityOutput {
    const message = createBaseEnsurePullabilityOutput();
    message.tableIdentifier = (object.tableIdentifier !== undefined && object.tableIdentifier !== null)
      ? TableIdentifier.fromPartial(object.tableIdentifier)
      : undefined;
    return message;
  },
};

function createBaseEnsurePullabilityBatchOutput(): EnsurePullabilityBatchOutput {
  return { tableIdentifierMapping: {} };
}

export const EnsurePullabilityBatchOutput = {
  encode(message: EnsurePullabilityBatchOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.tableIdentifierMapping).forEach(([key, value]) => {
      EnsurePullabilityBatchOutput_TableIdentifierMappingEntry.encode(
        { key: key as any, value },
        writer.uint32(10).fork(),
      ).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EnsurePullabilityBatchOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEnsurePullabilityBatchOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          const entry1 = EnsurePullabilityBatchOutput_TableIdentifierMappingEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.tableIdentifierMapping[entry1.key] = entry1.value;
          }
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EnsurePullabilityBatchOutput {
    return {
      tableIdentifierMapping: isObject(object.tableIdentifierMapping)
        ? Object.entries(object.tableIdentifierMapping).reduce<{ [key: string]: TableIdentifier }>(
          (acc, [key, value]) => {
            acc[key] = TableIdentifier.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
    };
  },

  toJSON(message: EnsurePullabilityBatchOutput): unknown {
    const obj: any = {};
    if (message.tableIdentifierMapping) {
      const entries = Object.entries(message.tableIdentifierMapping);
      if (entries.length > 0) {
        obj.tableIdentifierMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableIdentifierMapping[k] = TableIdentifier.toJSON(v);
        });
      }
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EnsurePullabilityBatchOutput>, I>>(base?: I): EnsurePullabilityBatchOutput {
    return EnsurePullabilityBatchOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EnsurePullabilityBatchOutput>, I>>(object: I): EnsurePullabilityBatchOutput {
    const message = createBaseEnsurePullabilityBatchOutput();
    message.tableIdentifierMapping = Object.entries(object.tableIdentifierMapping ?? {}).reduce<
      { [key: string]: TableIdentifier }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[key] = TableIdentifier.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseEnsurePullabilityBatchOutput_TableIdentifierMappingEntry(): EnsurePullabilityBatchOutput_TableIdentifierMappingEntry {
  return { key: "", value: undefined };
}

export const EnsurePullabilityBatchOutput_TableIdentifierMappingEntry = {
  encode(
    message: EnsurePullabilityBatchOutput_TableIdentifierMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== undefined) {
      TableIdentifier.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EnsurePullabilityBatchOutput_TableIdentifierMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEnsurePullabilityBatchOutput_TableIdentifierMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = TableIdentifier.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EnsurePullabilityBatchOutput_TableIdentifierMappingEntry {
    return {
      key: isSet(object.key) ? String(object.key) : "",
      value: isSet(object.value) ? TableIdentifier.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: EnsurePullabilityBatchOutput_TableIdentifierMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== undefined) {
      obj.value = TableIdentifier.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EnsurePullabilityBatchOutput_TableIdentifierMappingEntry>, I>>(
    base?: I,
  ): EnsurePullabilityBatchOutput_TableIdentifierMappingEntry {
    return EnsurePullabilityBatchOutput_TableIdentifierMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EnsurePullabilityBatchOutput_TableIdentifierMappingEntry>, I>>(
    object: I,
  ): EnsurePullabilityBatchOutput_TableIdentifierMappingEntry {
    const message = createBaseEnsurePullabilityBatchOutput_TableIdentifierMappingEntry();
    message.key = object.key ?? "";
    message.value = (object.value !== undefined && object.value !== null)
      ? TableIdentifier.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseSetupReplicationInput(): SetupReplicationInput {
  return {
    peerConnectionConfig: undefined,
    flowJobName: "",
    tableNameMapping: {},
    destinationPeer: undefined,
    doInitialCopy: false,
    existingPublicationName: "",
    existingReplicationSlotName: "",
  };
}

export const SetupReplicationInput = {
  encode(message: SetupReplicationInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    Object.entries(message.tableNameMapping).forEach(([key, value]) => {
      SetupReplicationInput_TableNameMappingEntry.encode({ key: key as any, value }, writer.uint32(26).fork()).ldelim();
    });
    if (message.destinationPeer !== undefined) {
      Peer.encode(message.destinationPeer, writer.uint32(34).fork()).ldelim();
    }
    if (message.doInitialCopy === true) {
      writer.uint32(40).bool(message.doInitialCopy);
    }
    if (message.existingPublicationName !== "") {
      writer.uint32(50).string(message.existingPublicationName);
    }
    if (message.existingReplicationSlotName !== "") {
      writer.uint32(58).string(message.existingReplicationSlotName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupReplicationInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupReplicationInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          const entry3 = SetupReplicationInput_TableNameMappingEntry.decode(reader, reader.uint32());
          if (entry3.value !== undefined) {
            message.tableNameMapping[entry3.key] = entry3.value;
          }
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.destinationPeer = Peer.decode(reader, reader.uint32());
          continue;
        case 5:
          if (tag !== 40) {
            break;
          }

          message.doInitialCopy = reader.bool();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.existingPublicationName = reader.string();
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.existingReplicationSlotName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupReplicationInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      tableNameMapping: isObject(object.tableNameMapping)
        ? Object.entries(object.tableNameMapping).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      destinationPeer: isSet(object.destinationPeer) ? Peer.fromJSON(object.destinationPeer) : undefined,
      doInitialCopy: isSet(object.doInitialCopy) ? Boolean(object.doInitialCopy) : false,
      existingPublicationName: isSet(object.existingPublicationName) ? String(object.existingPublicationName) : "",
      existingReplicationSlotName: isSet(object.existingReplicationSlotName)
        ? String(object.existingReplicationSlotName)
        : "",
    };
  },

  toJSON(message: SetupReplicationInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.tableNameMapping) {
      const entries = Object.entries(message.tableNameMapping);
      if (entries.length > 0) {
        obj.tableNameMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableNameMapping[k] = v;
        });
      }
    }
    if (message.destinationPeer !== undefined) {
      obj.destinationPeer = Peer.toJSON(message.destinationPeer);
    }
    if (message.doInitialCopy === true) {
      obj.doInitialCopy = message.doInitialCopy;
    }
    if (message.existingPublicationName !== "") {
      obj.existingPublicationName = message.existingPublicationName;
    }
    if (message.existingReplicationSlotName !== "") {
      obj.existingReplicationSlotName = message.existingReplicationSlotName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupReplicationInput>, I>>(base?: I): SetupReplicationInput {
    return SetupReplicationInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupReplicationInput>, I>>(object: I): SetupReplicationInput {
    const message = createBaseSetupReplicationInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.flowJobName = object.flowJobName ?? "";
    message.tableNameMapping = Object.entries(object.tableNameMapping ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.destinationPeer = (object.destinationPeer !== undefined && object.destinationPeer !== null)
      ? Peer.fromPartial(object.destinationPeer)
      : undefined;
    message.doInitialCopy = object.doInitialCopy ?? false;
    message.existingPublicationName = object.existingPublicationName ?? "";
    message.existingReplicationSlotName = object.existingReplicationSlotName ?? "";
    return message;
  },
};

function createBaseSetupReplicationInput_TableNameMappingEntry(): SetupReplicationInput_TableNameMappingEntry {
  return { key: "", value: "" };
}

export const SetupReplicationInput_TableNameMappingEntry = {
  encode(message: SetupReplicationInput_TableNameMappingEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupReplicationInput_TableNameMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupReplicationInput_TableNameMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupReplicationInput_TableNameMappingEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: SetupReplicationInput_TableNameMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== "") {
      obj.value = message.value;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupReplicationInput_TableNameMappingEntry>, I>>(
    base?: I,
  ): SetupReplicationInput_TableNameMappingEntry {
    return SetupReplicationInput_TableNameMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupReplicationInput_TableNameMappingEntry>, I>>(
    object: I,
  ): SetupReplicationInput_TableNameMappingEntry {
    const message = createBaseSetupReplicationInput_TableNameMappingEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSetupReplicationOutput(): SetupReplicationOutput {
  return { slotName: "", snapshotName: "" };
}

export const SetupReplicationOutput = {
  encode(message: SetupReplicationOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.slotName !== "") {
      writer.uint32(10).string(message.slotName);
    }
    if (message.snapshotName !== "") {
      writer.uint32(18).string(message.snapshotName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupReplicationOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupReplicationOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.slotName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.snapshotName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupReplicationOutput {
    return {
      slotName: isSet(object.slotName) ? String(object.slotName) : "",
      snapshotName: isSet(object.snapshotName) ? String(object.snapshotName) : "",
    };
  },

  toJSON(message: SetupReplicationOutput): unknown {
    const obj: any = {};
    if (message.slotName !== "") {
      obj.slotName = message.slotName;
    }
    if (message.snapshotName !== "") {
      obj.snapshotName = message.snapshotName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupReplicationOutput>, I>>(base?: I): SetupReplicationOutput {
    return SetupReplicationOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupReplicationOutput>, I>>(object: I): SetupReplicationOutput {
    const message = createBaseSetupReplicationOutput();
    message.slotName = object.slotName ?? "";
    message.snapshotName = object.snapshotName ?? "";
    return message;
  },
};

function createBaseCreateRawTableInput(): CreateRawTableInput {
  return { peerConnectionConfig: undefined, flowJobName: "", tableNameMapping: {}, cdcSyncMode: 0 };
}

export const CreateRawTableInput = {
  encode(message: CreateRawTableInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    Object.entries(message.tableNameMapping).forEach(([key, value]) => {
      CreateRawTableInput_TableNameMappingEntry.encode({ key: key as any, value }, writer.uint32(26).fork()).ldelim();
    });
    if (message.cdcSyncMode !== 0) {
      writer.uint32(32).int32(message.cdcSyncMode);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateRawTableInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateRawTableInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          const entry3 = CreateRawTableInput_TableNameMappingEntry.decode(reader, reader.uint32());
          if (entry3.value !== undefined) {
            message.tableNameMapping[entry3.key] = entry3.value;
          }
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.cdcSyncMode = reader.int32() as any;
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateRawTableInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      tableNameMapping: isObject(object.tableNameMapping)
        ? Object.entries(object.tableNameMapping).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      cdcSyncMode: isSet(object.cdcSyncMode) ? qRepSyncModeFromJSON(object.cdcSyncMode) : 0,
    };
  },

  toJSON(message: CreateRawTableInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.tableNameMapping) {
      const entries = Object.entries(message.tableNameMapping);
      if (entries.length > 0) {
        obj.tableNameMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableNameMapping[k] = v;
        });
      }
    }
    if (message.cdcSyncMode !== 0) {
      obj.cdcSyncMode = qRepSyncModeToJSON(message.cdcSyncMode);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateRawTableInput>, I>>(base?: I): CreateRawTableInput {
    return CreateRawTableInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateRawTableInput>, I>>(object: I): CreateRawTableInput {
    const message = createBaseCreateRawTableInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.flowJobName = object.flowJobName ?? "";
    message.tableNameMapping = Object.entries(object.tableNameMapping ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.cdcSyncMode = object.cdcSyncMode ?? 0;
    return message;
  },
};

function createBaseCreateRawTableInput_TableNameMappingEntry(): CreateRawTableInput_TableNameMappingEntry {
  return { key: "", value: "" };
}

export const CreateRawTableInput_TableNameMappingEntry = {
  encode(message: CreateRawTableInput_TableNameMappingEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateRawTableInput_TableNameMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateRawTableInput_TableNameMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateRawTableInput_TableNameMappingEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: CreateRawTableInput_TableNameMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== "") {
      obj.value = message.value;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateRawTableInput_TableNameMappingEntry>, I>>(
    base?: I,
  ): CreateRawTableInput_TableNameMappingEntry {
    return CreateRawTableInput_TableNameMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateRawTableInput_TableNameMappingEntry>, I>>(
    object: I,
  ): CreateRawTableInput_TableNameMappingEntry {
    const message = createBaseCreateRawTableInput_TableNameMappingEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseCreateRawTableOutput(): CreateRawTableOutput {
  return { tableIdentifier: "" };
}

export const CreateRawTableOutput = {
  encode(message: CreateRawTableOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableIdentifier !== "") {
      writer.uint32(10).string(message.tableIdentifier);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateRawTableOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateRawTableOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.tableIdentifier = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateRawTableOutput {
    return { tableIdentifier: isSet(object.tableIdentifier) ? String(object.tableIdentifier) : "" };
  },

  toJSON(message: CreateRawTableOutput): unknown {
    const obj: any = {};
    if (message.tableIdentifier !== "") {
      obj.tableIdentifier = message.tableIdentifier;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateRawTableOutput>, I>>(base?: I): CreateRawTableOutput {
    return CreateRawTableOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateRawTableOutput>, I>>(object: I): CreateRawTableOutput {
    const message = createBaseCreateRawTableOutput();
    message.tableIdentifier = object.tableIdentifier ?? "";
    return message;
  },
};

function createBaseTableSchema(): TableSchema {
  return { tableIdentifier: "", columns: {}, primaryKeyColumns: [], isReplicaIdentityFull: false };
}

export const TableSchema = {
  encode(message: TableSchema, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableIdentifier !== "") {
      writer.uint32(10).string(message.tableIdentifier);
    }
    Object.entries(message.columns).forEach(([key, value]) => {
      TableSchema_ColumnsEntry.encode({ key: key as any, value }, writer.uint32(18).fork()).ldelim();
    });
    for (const v of message.primaryKeyColumns) {
      writer.uint32(26).string(v!);
    }
    if (message.isReplicaIdentityFull === true) {
      writer.uint32(32).bool(message.isReplicaIdentityFull);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableSchema {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableSchema();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.tableIdentifier = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          const entry2 = TableSchema_ColumnsEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.columns[entry2.key] = entry2.value;
          }
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.primaryKeyColumns.push(reader.string());
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.isReplicaIdentityFull = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TableSchema {
    return {
      tableIdentifier: isSet(object.tableIdentifier) ? String(object.tableIdentifier) : "",
      columns: isObject(object.columns)
        ? Object.entries(object.columns).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      primaryKeyColumns: Array.isArray(object?.primaryKeyColumns)
        ? object.primaryKeyColumns.map((e: any) => String(e))
        : [],
      isReplicaIdentityFull: isSet(object.isReplicaIdentityFull) ? Boolean(object.isReplicaIdentityFull) : false,
    };
  },

  toJSON(message: TableSchema): unknown {
    const obj: any = {};
    if (message.tableIdentifier !== "") {
      obj.tableIdentifier = message.tableIdentifier;
    }
    if (message.columns) {
      const entries = Object.entries(message.columns);
      if (entries.length > 0) {
        obj.columns = {};
        entries.forEach(([k, v]) => {
          obj.columns[k] = v;
        });
      }
    }
    if (message.primaryKeyColumns?.length) {
      obj.primaryKeyColumns = message.primaryKeyColumns;
    }
    if (message.isReplicaIdentityFull === true) {
      obj.isReplicaIdentityFull = message.isReplicaIdentityFull;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TableSchema>, I>>(base?: I): TableSchema {
    return TableSchema.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TableSchema>, I>>(object: I): TableSchema {
    const message = createBaseTableSchema();
    message.tableIdentifier = object.tableIdentifier ?? "";
    message.columns = Object.entries(object.columns ?? {}).reduce<{ [key: string]: string }>((acc, [key, value]) => {
      if (value !== undefined) {
        acc[key] = String(value);
      }
      return acc;
    }, {});
    message.primaryKeyColumns = object.primaryKeyColumns?.map((e) => e) || [];
    message.isReplicaIdentityFull = object.isReplicaIdentityFull ?? false;
    return message;
  },
};

function createBaseTableSchema_ColumnsEntry(): TableSchema_ColumnsEntry {
  return { key: "", value: "" };
}

export const TableSchema_ColumnsEntry = {
  encode(message: TableSchema_ColumnsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableSchema_ColumnsEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableSchema_ColumnsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TableSchema_ColumnsEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: TableSchema_ColumnsEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== "") {
      obj.value = message.value;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TableSchema_ColumnsEntry>, I>>(base?: I): TableSchema_ColumnsEntry {
    return TableSchema_ColumnsEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TableSchema_ColumnsEntry>, I>>(object: I): TableSchema_ColumnsEntry {
    const message = createBaseTableSchema_ColumnsEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseGetTableSchemaBatchInput(): GetTableSchemaBatchInput {
  return { peerConnectionConfig: undefined, tableIdentifiers: [] };
}

export const GetTableSchemaBatchInput = {
  encode(message: GetTableSchemaBatchInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.tableIdentifiers) {
      writer.uint32(18).string(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetTableSchemaBatchInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetTableSchemaBatchInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.tableIdentifiers.push(reader.string());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): GetTableSchemaBatchInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      tableIdentifiers: Array.isArray(object?.tableIdentifiers)
        ? object.tableIdentifiers.map((e: any) => String(e))
        : [],
    };
  },

  toJSON(message: GetTableSchemaBatchInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.tableIdentifiers?.length) {
      obj.tableIdentifiers = message.tableIdentifiers;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<GetTableSchemaBatchInput>, I>>(base?: I): GetTableSchemaBatchInput {
    return GetTableSchemaBatchInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<GetTableSchemaBatchInput>, I>>(object: I): GetTableSchemaBatchInput {
    const message = createBaseGetTableSchemaBatchInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.tableIdentifiers = object.tableIdentifiers?.map((e) => e) || [];
    return message;
  },
};

function createBaseGetTableSchemaBatchOutput(): GetTableSchemaBatchOutput {
  return { tableNameSchemaMapping: {} };
}

export const GetTableSchemaBatchOutput = {
  encode(message: GetTableSchemaBatchOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.tableNameSchemaMapping).forEach(([key, value]) => {
      GetTableSchemaBatchOutput_TableNameSchemaMappingEntry.encode({ key: key as any, value }, writer.uint32(10).fork())
        .ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetTableSchemaBatchOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetTableSchemaBatchOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          const entry1 = GetTableSchemaBatchOutput_TableNameSchemaMappingEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.tableNameSchemaMapping[entry1.key] = entry1.value;
          }
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): GetTableSchemaBatchOutput {
    return {
      tableNameSchemaMapping: isObject(object.tableNameSchemaMapping)
        ? Object.entries(object.tableNameSchemaMapping).reduce<{ [key: string]: TableSchema }>((acc, [key, value]) => {
          acc[key] = TableSchema.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: GetTableSchemaBatchOutput): unknown {
    const obj: any = {};
    if (message.tableNameSchemaMapping) {
      const entries = Object.entries(message.tableNameSchemaMapping);
      if (entries.length > 0) {
        obj.tableNameSchemaMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableNameSchemaMapping[k] = TableSchema.toJSON(v);
        });
      }
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<GetTableSchemaBatchOutput>, I>>(base?: I): GetTableSchemaBatchOutput {
    return GetTableSchemaBatchOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<GetTableSchemaBatchOutput>, I>>(object: I): GetTableSchemaBatchOutput {
    const message = createBaseGetTableSchemaBatchOutput();
    message.tableNameSchemaMapping = Object.entries(object.tableNameSchemaMapping ?? {}).reduce<
      { [key: string]: TableSchema }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[key] = TableSchema.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseGetTableSchemaBatchOutput_TableNameSchemaMappingEntry(): GetTableSchemaBatchOutput_TableNameSchemaMappingEntry {
  return { key: "", value: undefined };
}

export const GetTableSchemaBatchOutput_TableNameSchemaMappingEntry = {
  encode(
    message: GetTableSchemaBatchOutput_TableNameSchemaMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== undefined) {
      TableSchema.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetTableSchemaBatchOutput_TableNameSchemaMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetTableSchemaBatchOutput_TableNameSchemaMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = TableSchema.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): GetTableSchemaBatchOutput_TableNameSchemaMappingEntry {
    return {
      key: isSet(object.key) ? String(object.key) : "",
      value: isSet(object.value) ? TableSchema.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: GetTableSchemaBatchOutput_TableNameSchemaMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== undefined) {
      obj.value = TableSchema.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<GetTableSchemaBatchOutput_TableNameSchemaMappingEntry>, I>>(
    base?: I,
  ): GetTableSchemaBatchOutput_TableNameSchemaMappingEntry {
    return GetTableSchemaBatchOutput_TableNameSchemaMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<GetTableSchemaBatchOutput_TableNameSchemaMappingEntry>, I>>(
    object: I,
  ): GetTableSchemaBatchOutput_TableNameSchemaMappingEntry {
    const message = createBaseGetTableSchemaBatchOutput_TableNameSchemaMappingEntry();
    message.key = object.key ?? "";
    message.value = (object.value !== undefined && object.value !== null)
      ? TableSchema.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseSetupNormalizedTableInput(): SetupNormalizedTableInput {
  return { peerConnectionConfig: undefined, tableIdentifier: "", sourceTableSchema: undefined };
}

export const SetupNormalizedTableInput = {
  encode(message: SetupNormalizedTableInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.tableIdentifier !== "") {
      writer.uint32(18).string(message.tableIdentifier);
    }
    if (message.sourceTableSchema !== undefined) {
      TableSchema.encode(message.sourceTableSchema, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupNormalizedTableInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupNormalizedTableInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.tableIdentifier = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.sourceTableSchema = TableSchema.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupNormalizedTableInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      tableIdentifier: isSet(object.tableIdentifier) ? String(object.tableIdentifier) : "",
      sourceTableSchema: isSet(object.sourceTableSchema) ? TableSchema.fromJSON(object.sourceTableSchema) : undefined,
    };
  },

  toJSON(message: SetupNormalizedTableInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.tableIdentifier !== "") {
      obj.tableIdentifier = message.tableIdentifier;
    }
    if (message.sourceTableSchema !== undefined) {
      obj.sourceTableSchema = TableSchema.toJSON(message.sourceTableSchema);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupNormalizedTableInput>, I>>(base?: I): SetupNormalizedTableInput {
    return SetupNormalizedTableInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupNormalizedTableInput>, I>>(object: I): SetupNormalizedTableInput {
    const message = createBaseSetupNormalizedTableInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.tableIdentifier = object.tableIdentifier ?? "";
    message.sourceTableSchema = (object.sourceTableSchema !== undefined && object.sourceTableSchema !== null)
      ? TableSchema.fromPartial(object.sourceTableSchema)
      : undefined;
    return message;
  },
};

function createBaseSetupNormalizedTableBatchInput(): SetupNormalizedTableBatchInput {
  return { peerConnectionConfig: undefined, tableNameSchemaMapping: {}, softDeleteColName: "", syncedAtColName: "" };
}

export const SetupNormalizedTableBatchInput = {
  encode(message: SetupNormalizedTableBatchInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerConnectionConfig !== undefined) {
      Peer.encode(message.peerConnectionConfig, writer.uint32(10).fork()).ldelim();
    }
    Object.entries(message.tableNameSchemaMapping).forEach(([key, value]) => {
      SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry.encode(
        { key: key as any, value },
        writer.uint32(18).fork(),
      ).ldelim();
    });
    if (message.softDeleteColName !== "") {
      writer.uint32(34).string(message.softDeleteColName);
    }
    if (message.syncedAtColName !== "") {
      writer.uint32(42).string(message.syncedAtColName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupNormalizedTableBatchInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupNormalizedTableBatchInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerConnectionConfig = Peer.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          const entry2 = SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.tableNameSchemaMapping[entry2.key] = entry2.value;
          }
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.softDeleteColName = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.syncedAtColName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupNormalizedTableBatchInput {
    return {
      peerConnectionConfig: isSet(object.peerConnectionConfig) ? Peer.fromJSON(object.peerConnectionConfig) : undefined,
      tableNameSchemaMapping: isObject(object.tableNameSchemaMapping)
        ? Object.entries(object.tableNameSchemaMapping).reduce<{ [key: string]: TableSchema }>((acc, [key, value]) => {
          acc[key] = TableSchema.fromJSON(value);
          return acc;
        }, {})
        : {},
      softDeleteColName: isSet(object.softDeleteColName) ? String(object.softDeleteColName) : "",
      syncedAtColName: isSet(object.syncedAtColName) ? String(object.syncedAtColName) : "",
    };
  },

  toJSON(message: SetupNormalizedTableBatchInput): unknown {
    const obj: any = {};
    if (message.peerConnectionConfig !== undefined) {
      obj.peerConnectionConfig = Peer.toJSON(message.peerConnectionConfig);
    }
    if (message.tableNameSchemaMapping) {
      const entries = Object.entries(message.tableNameSchemaMapping);
      if (entries.length > 0) {
        obj.tableNameSchemaMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableNameSchemaMapping[k] = TableSchema.toJSON(v);
        });
      }
    }
    if (message.softDeleteColName !== "") {
      obj.softDeleteColName = message.softDeleteColName;
    }
    if (message.syncedAtColName !== "") {
      obj.syncedAtColName = message.syncedAtColName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupNormalizedTableBatchInput>, I>>(base?: I): SetupNormalizedTableBatchInput {
    return SetupNormalizedTableBatchInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupNormalizedTableBatchInput>, I>>(
    object: I,
  ): SetupNormalizedTableBatchInput {
    const message = createBaseSetupNormalizedTableBatchInput();
    message.peerConnectionConfig = (object.peerConnectionConfig !== undefined && object.peerConnectionConfig !== null)
      ? Peer.fromPartial(object.peerConnectionConfig)
      : undefined;
    message.tableNameSchemaMapping = Object.entries(object.tableNameSchemaMapping ?? {}).reduce<
      { [key: string]: TableSchema }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[key] = TableSchema.fromPartial(value);
      }
      return acc;
    }, {});
    message.softDeleteColName = object.softDeleteColName ?? "";
    message.syncedAtColName = object.syncedAtColName ?? "";
    return message;
  },
};

function createBaseSetupNormalizedTableBatchInput_TableNameSchemaMappingEntry(): SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry {
  return { key: "", value: undefined };
}

export const SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry = {
  encode(
    message: SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== undefined) {
      TableSchema.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupNormalizedTableBatchInput_TableNameSchemaMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.value = TableSchema.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry {
    return {
      key: isSet(object.key) ? String(object.key) : "",
      value: isSet(object.value) ? TableSchema.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== undefined) {
      obj.value = TableSchema.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry>, I>>(
    base?: I,
  ): SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry {
    return SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry>, I>>(
    object: I,
  ): SetupNormalizedTableBatchInput_TableNameSchemaMappingEntry {
    const message = createBaseSetupNormalizedTableBatchInput_TableNameSchemaMappingEntry();
    message.key = object.key ?? "";
    message.value = (object.value !== undefined && object.value !== null)
      ? TableSchema.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseSetupNormalizedTableOutput(): SetupNormalizedTableOutput {
  return { tableIdentifier: "", alreadyExists: false };
}

export const SetupNormalizedTableOutput = {
  encode(message: SetupNormalizedTableOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableIdentifier !== "") {
      writer.uint32(10).string(message.tableIdentifier);
    }
    if (message.alreadyExists === true) {
      writer.uint32(16).bool(message.alreadyExists);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupNormalizedTableOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupNormalizedTableOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.tableIdentifier = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.alreadyExists = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupNormalizedTableOutput {
    return {
      tableIdentifier: isSet(object.tableIdentifier) ? String(object.tableIdentifier) : "",
      alreadyExists: isSet(object.alreadyExists) ? Boolean(object.alreadyExists) : false,
    };
  },

  toJSON(message: SetupNormalizedTableOutput): unknown {
    const obj: any = {};
    if (message.tableIdentifier !== "") {
      obj.tableIdentifier = message.tableIdentifier;
    }
    if (message.alreadyExists === true) {
      obj.alreadyExists = message.alreadyExists;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupNormalizedTableOutput>, I>>(base?: I): SetupNormalizedTableOutput {
    return SetupNormalizedTableOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupNormalizedTableOutput>, I>>(object: I): SetupNormalizedTableOutput {
    const message = createBaseSetupNormalizedTableOutput();
    message.tableIdentifier = object.tableIdentifier ?? "";
    message.alreadyExists = object.alreadyExists ?? false;
    return message;
  },
};

function createBaseSetupNormalizedTableBatchOutput(): SetupNormalizedTableBatchOutput {
  return { tableExistsMapping: {} };
}

export const SetupNormalizedTableBatchOutput = {
  encode(message: SetupNormalizedTableBatchOutput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.tableExistsMapping).forEach(([key, value]) => {
      SetupNormalizedTableBatchOutput_TableExistsMappingEntry.encode(
        { key: key as any, value },
        writer.uint32(10).fork(),
      ).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupNormalizedTableBatchOutput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupNormalizedTableBatchOutput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          const entry1 = SetupNormalizedTableBatchOutput_TableExistsMappingEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.tableExistsMapping[entry1.key] = entry1.value;
          }
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupNormalizedTableBatchOutput {
    return {
      tableExistsMapping: isObject(object.tableExistsMapping)
        ? Object.entries(object.tableExistsMapping).reduce<{ [key: string]: boolean }>((acc, [key, value]) => {
          acc[key] = Boolean(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: SetupNormalizedTableBatchOutput): unknown {
    const obj: any = {};
    if (message.tableExistsMapping) {
      const entries = Object.entries(message.tableExistsMapping);
      if (entries.length > 0) {
        obj.tableExistsMapping = {};
        entries.forEach(([k, v]) => {
          obj.tableExistsMapping[k] = v;
        });
      }
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupNormalizedTableBatchOutput>, I>>(base?: I): SetupNormalizedTableBatchOutput {
    return SetupNormalizedTableBatchOutput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupNormalizedTableBatchOutput>, I>>(
    object: I,
  ): SetupNormalizedTableBatchOutput {
    const message = createBaseSetupNormalizedTableBatchOutput();
    message.tableExistsMapping = Object.entries(object.tableExistsMapping ?? {}).reduce<{ [key: string]: boolean }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = Boolean(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseSetupNormalizedTableBatchOutput_TableExistsMappingEntry(): SetupNormalizedTableBatchOutput_TableExistsMappingEntry {
  return { key: "", value: false };
}

export const SetupNormalizedTableBatchOutput_TableExistsMappingEntry = {
  encode(
    message: SetupNormalizedTableBatchOutput_TableExistsMappingEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value === true) {
      writer.uint32(16).bool(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SetupNormalizedTableBatchOutput_TableExistsMappingEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSetupNormalizedTableBatchOutput_TableExistsMappingEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.key = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.value = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SetupNormalizedTableBatchOutput_TableExistsMappingEntry {
    return {
      key: isSet(object.key) ? String(object.key) : "",
      value: isSet(object.value) ? Boolean(object.value) : false,
    };
  },

  toJSON(message: SetupNormalizedTableBatchOutput_TableExistsMappingEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value === true) {
      obj.value = message.value;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SetupNormalizedTableBatchOutput_TableExistsMappingEntry>, I>>(
    base?: I,
  ): SetupNormalizedTableBatchOutput_TableExistsMappingEntry {
    return SetupNormalizedTableBatchOutput_TableExistsMappingEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SetupNormalizedTableBatchOutput_TableExistsMappingEntry>, I>>(
    object: I,
  ): SetupNormalizedTableBatchOutput_TableExistsMappingEntry {
    const message = createBaseSetupNormalizedTableBatchOutput_TableExistsMappingEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? false;
    return message;
  },
};

function createBaseIntPartitionRange(): IntPartitionRange {
  return { start: 0, end: 0 };
}

export const IntPartitionRange = {
  encode(message: IntPartitionRange, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.start !== 0) {
      writer.uint32(8).int64(message.start);
    }
    if (message.end !== 0) {
      writer.uint32(16).int64(message.end);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): IntPartitionRange {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseIntPartitionRange();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.start = longToNumber(reader.int64() as Long);
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.end = longToNumber(reader.int64() as Long);
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): IntPartitionRange {
    return { start: isSet(object.start) ? Number(object.start) : 0, end: isSet(object.end) ? Number(object.end) : 0 };
  },

  toJSON(message: IntPartitionRange): unknown {
    const obj: any = {};
    if (message.start !== 0) {
      obj.start = Math.round(message.start);
    }
    if (message.end !== 0) {
      obj.end = Math.round(message.end);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<IntPartitionRange>, I>>(base?: I): IntPartitionRange {
    return IntPartitionRange.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<IntPartitionRange>, I>>(object: I): IntPartitionRange {
    const message = createBaseIntPartitionRange();
    message.start = object.start ?? 0;
    message.end = object.end ?? 0;
    return message;
  },
};

function createBaseTimestampPartitionRange(): TimestampPartitionRange {
  return { start: undefined, end: undefined };
}

export const TimestampPartitionRange = {
  encode(message: TimestampPartitionRange, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.start !== undefined) {
      Timestamp.encode(toTimestamp(message.start), writer.uint32(10).fork()).ldelim();
    }
    if (message.end !== undefined) {
      Timestamp.encode(toTimestamp(message.end), writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TimestampPartitionRange {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTimestampPartitionRange();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.start = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.end = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TimestampPartitionRange {
    return {
      start: isSet(object.start) ? fromJsonTimestamp(object.start) : undefined,
      end: isSet(object.end) ? fromJsonTimestamp(object.end) : undefined,
    };
  },

  toJSON(message: TimestampPartitionRange): unknown {
    const obj: any = {};
    if (message.start !== undefined) {
      obj.start = message.start.toISOString();
    }
    if (message.end !== undefined) {
      obj.end = message.end.toISOString();
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TimestampPartitionRange>, I>>(base?: I): TimestampPartitionRange {
    return TimestampPartitionRange.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TimestampPartitionRange>, I>>(object: I): TimestampPartitionRange {
    const message = createBaseTimestampPartitionRange();
    message.start = object.start ?? undefined;
    message.end = object.end ?? undefined;
    return message;
  },
};

function createBaseTID(): TID {
  return { blockNumber: 0, offsetNumber: 0 };
}

export const TID = {
  encode(message: TID, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.blockNumber !== 0) {
      writer.uint32(8).uint32(message.blockNumber);
    }
    if (message.offsetNumber !== 0) {
      writer.uint32(16).uint32(message.offsetNumber);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TID {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTID();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.blockNumber = reader.uint32();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.offsetNumber = reader.uint32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TID {
    return {
      blockNumber: isSet(object.blockNumber) ? Number(object.blockNumber) : 0,
      offsetNumber: isSet(object.offsetNumber) ? Number(object.offsetNumber) : 0,
    };
  },

  toJSON(message: TID): unknown {
    const obj: any = {};
    if (message.blockNumber !== 0) {
      obj.blockNumber = Math.round(message.blockNumber);
    }
    if (message.offsetNumber !== 0) {
      obj.offsetNumber = Math.round(message.offsetNumber);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TID>, I>>(base?: I): TID {
    return TID.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TID>, I>>(object: I): TID {
    const message = createBaseTID();
    message.blockNumber = object.blockNumber ?? 0;
    message.offsetNumber = object.offsetNumber ?? 0;
    return message;
  },
};

function createBaseTIDPartitionRange(): TIDPartitionRange {
  return { start: undefined, end: undefined };
}

export const TIDPartitionRange = {
  encode(message: TIDPartitionRange, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.start !== undefined) {
      TID.encode(message.start, writer.uint32(10).fork()).ldelim();
    }
    if (message.end !== undefined) {
      TID.encode(message.end, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TIDPartitionRange {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTIDPartitionRange();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.start = TID.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.end = TID.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TIDPartitionRange {
    return {
      start: isSet(object.start) ? TID.fromJSON(object.start) : undefined,
      end: isSet(object.end) ? TID.fromJSON(object.end) : undefined,
    };
  },

  toJSON(message: TIDPartitionRange): unknown {
    const obj: any = {};
    if (message.start !== undefined) {
      obj.start = TID.toJSON(message.start);
    }
    if (message.end !== undefined) {
      obj.end = TID.toJSON(message.end);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TIDPartitionRange>, I>>(base?: I): TIDPartitionRange {
    return TIDPartitionRange.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TIDPartitionRange>, I>>(object: I): TIDPartitionRange {
    const message = createBaseTIDPartitionRange();
    message.start = (object.start !== undefined && object.start !== null) ? TID.fromPartial(object.start) : undefined;
    message.end = (object.end !== undefined && object.end !== null) ? TID.fromPartial(object.end) : undefined;
    return message;
  },
};

function createBasePartitionRange(): PartitionRange {
  return { intRange: undefined, timestampRange: undefined, tidRange: undefined };
}

export const PartitionRange = {
  encode(message: PartitionRange, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.intRange !== undefined) {
      IntPartitionRange.encode(message.intRange, writer.uint32(10).fork()).ldelim();
    }
    if (message.timestampRange !== undefined) {
      TimestampPartitionRange.encode(message.timestampRange, writer.uint32(18).fork()).ldelim();
    }
    if (message.tidRange !== undefined) {
      TIDPartitionRange.encode(message.tidRange, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PartitionRange {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePartitionRange();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.intRange = IntPartitionRange.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.timestampRange = TimestampPartitionRange.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.tidRange = TIDPartitionRange.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PartitionRange {
    return {
      intRange: isSet(object.intRange) ? IntPartitionRange.fromJSON(object.intRange) : undefined,
      timestampRange: isSet(object.timestampRange)
        ? TimestampPartitionRange.fromJSON(object.timestampRange)
        : undefined,
      tidRange: isSet(object.tidRange) ? TIDPartitionRange.fromJSON(object.tidRange) : undefined,
    };
  },

  toJSON(message: PartitionRange): unknown {
    const obj: any = {};
    if (message.intRange !== undefined) {
      obj.intRange = IntPartitionRange.toJSON(message.intRange);
    }
    if (message.timestampRange !== undefined) {
      obj.timestampRange = TimestampPartitionRange.toJSON(message.timestampRange);
    }
    if (message.tidRange !== undefined) {
      obj.tidRange = TIDPartitionRange.toJSON(message.tidRange);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PartitionRange>, I>>(base?: I): PartitionRange {
    return PartitionRange.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PartitionRange>, I>>(object: I): PartitionRange {
    const message = createBasePartitionRange();
    message.intRange = (object.intRange !== undefined && object.intRange !== null)
      ? IntPartitionRange.fromPartial(object.intRange)
      : undefined;
    message.timestampRange = (object.timestampRange !== undefined && object.timestampRange !== null)
      ? TimestampPartitionRange.fromPartial(object.timestampRange)
      : undefined;
    message.tidRange = (object.tidRange !== undefined && object.tidRange !== null)
      ? TIDPartitionRange.fromPartial(object.tidRange)
      : undefined;
    return message;
  },
};

function createBaseQRepWriteMode(): QRepWriteMode {
  return { writeType: 0, upsertKeyColumns: [] };
}

export const QRepWriteMode = {
  encode(message: QRepWriteMode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.writeType !== 0) {
      writer.uint32(8).int32(message.writeType);
    }
    for (const v of message.upsertKeyColumns) {
      writer.uint32(18).string(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepWriteMode {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepWriteMode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.writeType = reader.int32() as any;
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.upsertKeyColumns.push(reader.string());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepWriteMode {
    return {
      writeType: isSet(object.writeType) ? qRepWriteTypeFromJSON(object.writeType) : 0,
      upsertKeyColumns: Array.isArray(object?.upsertKeyColumns)
        ? object.upsertKeyColumns.map((e: any) => String(e))
        : [],
    };
  },

  toJSON(message: QRepWriteMode): unknown {
    const obj: any = {};
    if (message.writeType !== 0) {
      obj.writeType = qRepWriteTypeToJSON(message.writeType);
    }
    if (message.upsertKeyColumns?.length) {
      obj.upsertKeyColumns = message.upsertKeyColumns;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepWriteMode>, I>>(base?: I): QRepWriteMode {
    return QRepWriteMode.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepWriteMode>, I>>(object: I): QRepWriteMode {
    const message = createBaseQRepWriteMode();
    message.writeType = object.writeType ?? 0;
    message.upsertKeyColumns = object.upsertKeyColumns?.map((e) => e) || [];
    return message;
  },
};

function createBaseQRepConfig(): QRepConfig {
  return {
    flowJobName: "",
    sourcePeer: undefined,
    destinationPeer: undefined,
    destinationTableIdentifier: "",
    query: "",
    watermarkTable: "",
    watermarkColumn: "",
    initialCopyOnly: false,
    syncMode: 0,
    batchSizeInt: 0,
    batchDurationSeconds: 0,
    maxParallelWorkers: 0,
    waitBetweenBatchesSeconds: 0,
    writeMode: undefined,
    stagingPath: "",
    numRowsPerPartition: 0,
    setupWatermarkTableOnDestination: false,
    dstTableFullResync: false,
  };
}

export const QRepConfig = {
  encode(message: QRepConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(10).string(message.flowJobName);
    }
    if (message.sourcePeer !== undefined) {
      Peer.encode(message.sourcePeer, writer.uint32(18).fork()).ldelim();
    }
    if (message.destinationPeer !== undefined) {
      Peer.encode(message.destinationPeer, writer.uint32(26).fork()).ldelim();
    }
    if (message.destinationTableIdentifier !== "") {
      writer.uint32(34).string(message.destinationTableIdentifier);
    }
    if (message.query !== "") {
      writer.uint32(42).string(message.query);
    }
    if (message.watermarkTable !== "") {
      writer.uint32(50).string(message.watermarkTable);
    }
    if (message.watermarkColumn !== "") {
      writer.uint32(58).string(message.watermarkColumn);
    }
    if (message.initialCopyOnly === true) {
      writer.uint32(64).bool(message.initialCopyOnly);
    }
    if (message.syncMode !== 0) {
      writer.uint32(72).int32(message.syncMode);
    }
    if (message.batchSizeInt !== 0) {
      writer.uint32(80).uint32(message.batchSizeInt);
    }
    if (message.batchDurationSeconds !== 0) {
      writer.uint32(88).uint32(message.batchDurationSeconds);
    }
    if (message.maxParallelWorkers !== 0) {
      writer.uint32(96).uint32(message.maxParallelWorkers);
    }
    if (message.waitBetweenBatchesSeconds !== 0) {
      writer.uint32(104).uint32(message.waitBetweenBatchesSeconds);
    }
    if (message.writeMode !== undefined) {
      QRepWriteMode.encode(message.writeMode, writer.uint32(114).fork()).ldelim();
    }
    if (message.stagingPath !== "") {
      writer.uint32(122).string(message.stagingPath);
    }
    if (message.numRowsPerPartition !== 0) {
      writer.uint32(128).uint32(message.numRowsPerPartition);
    }
    if (message.setupWatermarkTableOnDestination === true) {
      writer.uint32(136).bool(message.setupWatermarkTableOnDestination);
    }
    if (message.dstTableFullResync === true) {
      writer.uint32(144).bool(message.dstTableFullResync);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowJobName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.sourcePeer = Peer.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.destinationPeer = Peer.decode(reader, reader.uint32());
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.destinationTableIdentifier = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.query = reader.string();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.watermarkTable = reader.string();
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.watermarkColumn = reader.string();
          continue;
        case 8:
          if (tag !== 64) {
            break;
          }

          message.initialCopyOnly = reader.bool();
          continue;
        case 9:
          if (tag !== 72) {
            break;
          }

          message.syncMode = reader.int32() as any;
          continue;
        case 10:
          if (tag !== 80) {
            break;
          }

          message.batchSizeInt = reader.uint32();
          continue;
        case 11:
          if (tag !== 88) {
            break;
          }

          message.batchDurationSeconds = reader.uint32();
          continue;
        case 12:
          if (tag !== 96) {
            break;
          }

          message.maxParallelWorkers = reader.uint32();
          continue;
        case 13:
          if (tag !== 104) {
            break;
          }

          message.waitBetweenBatchesSeconds = reader.uint32();
          continue;
        case 14:
          if (tag !== 114) {
            break;
          }

          message.writeMode = QRepWriteMode.decode(reader, reader.uint32());
          continue;
        case 15:
          if (tag !== 122) {
            break;
          }

          message.stagingPath = reader.string();
          continue;
        case 16:
          if (tag !== 128) {
            break;
          }

          message.numRowsPerPartition = reader.uint32();
          continue;
        case 17:
          if (tag !== 136) {
            break;
          }

          message.setupWatermarkTableOnDestination = reader.bool();
          continue;
        case 18:
          if (tag !== 144) {
            break;
          }

          message.dstTableFullResync = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepConfig {
    return {
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      sourcePeer: isSet(object.sourcePeer) ? Peer.fromJSON(object.sourcePeer) : undefined,
      destinationPeer: isSet(object.destinationPeer) ? Peer.fromJSON(object.destinationPeer) : undefined,
      destinationTableIdentifier: isSet(object.destinationTableIdentifier)
        ? String(object.destinationTableIdentifier)
        : "",
      query: isSet(object.query) ? String(object.query) : "",
      watermarkTable: isSet(object.watermarkTable) ? String(object.watermarkTable) : "",
      watermarkColumn: isSet(object.watermarkColumn) ? String(object.watermarkColumn) : "",
      initialCopyOnly: isSet(object.initialCopyOnly) ? Boolean(object.initialCopyOnly) : false,
      syncMode: isSet(object.syncMode) ? qRepSyncModeFromJSON(object.syncMode) : 0,
      batchSizeInt: isSet(object.batchSizeInt) ? Number(object.batchSizeInt) : 0,
      batchDurationSeconds: isSet(object.batchDurationSeconds) ? Number(object.batchDurationSeconds) : 0,
      maxParallelWorkers: isSet(object.maxParallelWorkers) ? Number(object.maxParallelWorkers) : 0,
      waitBetweenBatchesSeconds: isSet(object.waitBetweenBatchesSeconds) ? Number(object.waitBetweenBatchesSeconds) : 0,
      writeMode: isSet(object.writeMode) ? QRepWriteMode.fromJSON(object.writeMode) : undefined,
      stagingPath: isSet(object.stagingPath) ? String(object.stagingPath) : "",
      numRowsPerPartition: isSet(object.numRowsPerPartition) ? Number(object.numRowsPerPartition) : 0,
      setupWatermarkTableOnDestination: isSet(object.setupWatermarkTableOnDestination)
        ? Boolean(object.setupWatermarkTableOnDestination)
        : false,
      dstTableFullResync: isSet(object.dstTableFullResync) ? Boolean(object.dstTableFullResync) : false,
    };
  },

  toJSON(message: QRepConfig): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.sourcePeer !== undefined) {
      obj.sourcePeer = Peer.toJSON(message.sourcePeer);
    }
    if (message.destinationPeer !== undefined) {
      obj.destinationPeer = Peer.toJSON(message.destinationPeer);
    }
    if (message.destinationTableIdentifier !== "") {
      obj.destinationTableIdentifier = message.destinationTableIdentifier;
    }
    if (message.query !== "") {
      obj.query = message.query;
    }
    if (message.watermarkTable !== "") {
      obj.watermarkTable = message.watermarkTable;
    }
    if (message.watermarkColumn !== "") {
      obj.watermarkColumn = message.watermarkColumn;
    }
    if (message.initialCopyOnly === true) {
      obj.initialCopyOnly = message.initialCopyOnly;
    }
    if (message.syncMode !== 0) {
      obj.syncMode = qRepSyncModeToJSON(message.syncMode);
    }
    if (message.batchSizeInt !== 0) {
      obj.batchSizeInt = Math.round(message.batchSizeInt);
    }
    if (message.batchDurationSeconds !== 0) {
      obj.batchDurationSeconds = Math.round(message.batchDurationSeconds);
    }
    if (message.maxParallelWorkers !== 0) {
      obj.maxParallelWorkers = Math.round(message.maxParallelWorkers);
    }
    if (message.waitBetweenBatchesSeconds !== 0) {
      obj.waitBetweenBatchesSeconds = Math.round(message.waitBetweenBatchesSeconds);
    }
    if (message.writeMode !== undefined) {
      obj.writeMode = QRepWriteMode.toJSON(message.writeMode);
    }
    if (message.stagingPath !== "") {
      obj.stagingPath = message.stagingPath;
    }
    if (message.numRowsPerPartition !== 0) {
      obj.numRowsPerPartition = Math.round(message.numRowsPerPartition);
    }
    if (message.setupWatermarkTableOnDestination === true) {
      obj.setupWatermarkTableOnDestination = message.setupWatermarkTableOnDestination;
    }
    if (message.dstTableFullResync === true) {
      obj.dstTableFullResync = message.dstTableFullResync;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepConfig>, I>>(base?: I): QRepConfig {
    return QRepConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepConfig>, I>>(object: I): QRepConfig {
    const message = createBaseQRepConfig();
    message.flowJobName = object.flowJobName ?? "";
    message.sourcePeer = (object.sourcePeer !== undefined && object.sourcePeer !== null)
      ? Peer.fromPartial(object.sourcePeer)
      : undefined;
    message.destinationPeer = (object.destinationPeer !== undefined && object.destinationPeer !== null)
      ? Peer.fromPartial(object.destinationPeer)
      : undefined;
    message.destinationTableIdentifier = object.destinationTableIdentifier ?? "";
    message.query = object.query ?? "";
    message.watermarkTable = object.watermarkTable ?? "";
    message.watermarkColumn = object.watermarkColumn ?? "";
    message.initialCopyOnly = object.initialCopyOnly ?? false;
    message.syncMode = object.syncMode ?? 0;
    message.batchSizeInt = object.batchSizeInt ?? 0;
    message.batchDurationSeconds = object.batchDurationSeconds ?? 0;
    message.maxParallelWorkers = object.maxParallelWorkers ?? 0;
    message.waitBetweenBatchesSeconds = object.waitBetweenBatchesSeconds ?? 0;
    message.writeMode = (object.writeMode !== undefined && object.writeMode !== null)
      ? QRepWriteMode.fromPartial(object.writeMode)
      : undefined;
    message.stagingPath = object.stagingPath ?? "";
    message.numRowsPerPartition = object.numRowsPerPartition ?? 0;
    message.setupWatermarkTableOnDestination = object.setupWatermarkTableOnDestination ?? false;
    message.dstTableFullResync = object.dstTableFullResync ?? false;
    return message;
  },
};

function createBaseQRepPartition(): QRepPartition {
  return { partitionId: "", range: undefined, fullTablePartition: false };
}

export const QRepPartition = {
  encode(message: QRepPartition, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.partitionId !== "") {
      writer.uint32(18).string(message.partitionId);
    }
    if (message.range !== undefined) {
      PartitionRange.encode(message.range, writer.uint32(26).fork()).ldelim();
    }
    if (message.fullTablePartition === true) {
      writer.uint32(32).bool(message.fullTablePartition);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepPartition {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepPartition();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 2:
          if (tag !== 18) {
            break;
          }

          message.partitionId = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.range = PartitionRange.decode(reader, reader.uint32());
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.fullTablePartition = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepPartition {
    return {
      partitionId: isSet(object.partitionId) ? String(object.partitionId) : "",
      range: isSet(object.range) ? PartitionRange.fromJSON(object.range) : undefined,
      fullTablePartition: isSet(object.fullTablePartition) ? Boolean(object.fullTablePartition) : false,
    };
  },

  toJSON(message: QRepPartition): unknown {
    const obj: any = {};
    if (message.partitionId !== "") {
      obj.partitionId = message.partitionId;
    }
    if (message.range !== undefined) {
      obj.range = PartitionRange.toJSON(message.range);
    }
    if (message.fullTablePartition === true) {
      obj.fullTablePartition = message.fullTablePartition;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepPartition>, I>>(base?: I): QRepPartition {
    return QRepPartition.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepPartition>, I>>(object: I): QRepPartition {
    const message = createBaseQRepPartition();
    message.partitionId = object.partitionId ?? "";
    message.range = (object.range !== undefined && object.range !== null)
      ? PartitionRange.fromPartial(object.range)
      : undefined;
    message.fullTablePartition = object.fullTablePartition ?? false;
    return message;
  },
};

function createBaseQRepPartitionBatch(): QRepPartitionBatch {
  return { batchId: 0, partitions: [] };
}

export const QRepPartitionBatch = {
  encode(message: QRepPartitionBatch, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.batchId !== 0) {
      writer.uint32(8).int32(message.batchId);
    }
    for (const v of message.partitions) {
      QRepPartition.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepPartitionBatch {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepPartitionBatch();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.batchId = reader.int32();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.partitions.push(QRepPartition.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepPartitionBatch {
    return {
      batchId: isSet(object.batchId) ? Number(object.batchId) : 0,
      partitions: Array.isArray(object?.partitions) ? object.partitions.map((e: any) => QRepPartition.fromJSON(e)) : [],
    };
  },

  toJSON(message: QRepPartitionBatch): unknown {
    const obj: any = {};
    if (message.batchId !== 0) {
      obj.batchId = Math.round(message.batchId);
    }
    if (message.partitions?.length) {
      obj.partitions = message.partitions.map((e) => QRepPartition.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepPartitionBatch>, I>>(base?: I): QRepPartitionBatch {
    return QRepPartitionBatch.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepPartitionBatch>, I>>(object: I): QRepPartitionBatch {
    const message = createBaseQRepPartitionBatch();
    message.batchId = object.batchId ?? 0;
    message.partitions = object.partitions?.map((e) => QRepPartition.fromPartial(e)) || [];
    return message;
  },
};

function createBaseQRepParitionResult(): QRepParitionResult {
  return { partitions: [] };
}

export const QRepParitionResult = {
  encode(message: QRepParitionResult, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.partitions) {
      QRepPartition.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepParitionResult {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepParitionResult();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.partitions.push(QRepPartition.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepParitionResult {
    return {
      partitions: Array.isArray(object?.partitions) ? object.partitions.map((e: any) => QRepPartition.fromJSON(e)) : [],
    };
  },

  toJSON(message: QRepParitionResult): unknown {
    const obj: any = {};
    if (message.partitions?.length) {
      obj.partitions = message.partitions.map((e) => QRepPartition.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepParitionResult>, I>>(base?: I): QRepParitionResult {
    return QRepParitionResult.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepParitionResult>, I>>(object: I): QRepParitionResult {
    const message = createBaseQRepParitionResult();
    message.partitions = object.partitions?.map((e) => QRepPartition.fromPartial(e)) || [];
    return message;
  },
};

function createBaseDropFlowInput(): DropFlowInput {
  return { flowName: "" };
}

export const DropFlowInput = {
  encode(message: DropFlowInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowName !== "") {
      writer.uint32(10).string(message.flowName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropFlowInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropFlowInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): DropFlowInput {
    return { flowName: isSet(object.flowName) ? String(object.flowName) : "" };
  },

  toJSON(message: DropFlowInput): unknown {
    const obj: any = {};
    if (message.flowName !== "") {
      obj.flowName = message.flowName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<DropFlowInput>, I>>(base?: I): DropFlowInput {
    return DropFlowInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<DropFlowInput>, I>>(object: I): DropFlowInput {
    const message = createBaseDropFlowInput();
    message.flowName = object.flowName ?? "";
    return message;
  },
};

function createBaseDeltaAddedColumn(): DeltaAddedColumn {
  return { columnName: "", columnType: "" };
}

export const DeltaAddedColumn = {
  encode(message: DeltaAddedColumn, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.columnName !== "") {
      writer.uint32(10).string(message.columnName);
    }
    if (message.columnType !== "") {
      writer.uint32(18).string(message.columnType);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DeltaAddedColumn {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDeltaAddedColumn();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.columnName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.columnType = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): DeltaAddedColumn {
    return {
      columnName: isSet(object.columnName) ? String(object.columnName) : "",
      columnType: isSet(object.columnType) ? String(object.columnType) : "",
    };
  },

  toJSON(message: DeltaAddedColumn): unknown {
    const obj: any = {};
    if (message.columnName !== "") {
      obj.columnName = message.columnName;
    }
    if (message.columnType !== "") {
      obj.columnType = message.columnType;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<DeltaAddedColumn>, I>>(base?: I): DeltaAddedColumn {
    return DeltaAddedColumn.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<DeltaAddedColumn>, I>>(object: I): DeltaAddedColumn {
    const message = createBaseDeltaAddedColumn();
    message.columnName = object.columnName ?? "";
    message.columnType = object.columnType ?? "";
    return message;
  },
};

function createBaseTableSchemaDelta(): TableSchemaDelta {
  return { srcTableName: "", dstTableName: "", addedColumns: [] };
}

export const TableSchemaDelta = {
  encode(message: TableSchemaDelta, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.srcTableName !== "") {
      writer.uint32(10).string(message.srcTableName);
    }
    if (message.dstTableName !== "") {
      writer.uint32(18).string(message.dstTableName);
    }
    for (const v of message.addedColumns) {
      DeltaAddedColumn.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableSchemaDelta {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableSchemaDelta();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.srcTableName = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.dstTableName = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.addedColumns.push(DeltaAddedColumn.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): TableSchemaDelta {
    return {
      srcTableName: isSet(object.srcTableName) ? String(object.srcTableName) : "",
      dstTableName: isSet(object.dstTableName) ? String(object.dstTableName) : "",
      addedColumns: Array.isArray(object?.addedColumns)
        ? object.addedColumns.map((e: any) => DeltaAddedColumn.fromJSON(e))
        : [],
    };
  },

  toJSON(message: TableSchemaDelta): unknown {
    const obj: any = {};
    if (message.srcTableName !== "") {
      obj.srcTableName = message.srcTableName;
    }
    if (message.dstTableName !== "") {
      obj.dstTableName = message.dstTableName;
    }
    if (message.addedColumns?.length) {
      obj.addedColumns = message.addedColumns.map((e) => DeltaAddedColumn.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<TableSchemaDelta>, I>>(base?: I): TableSchemaDelta {
    return TableSchemaDelta.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<TableSchemaDelta>, I>>(object: I): TableSchemaDelta {
    const message = createBaseTableSchemaDelta();
    message.srcTableName = object.srcTableName ?? "";
    message.dstTableName = object.dstTableName ?? "";
    message.addedColumns = object.addedColumns?.map((e) => DeltaAddedColumn.fromPartial(e)) || [];
    return message;
  },
};

function createBaseReplayTableSchemaDeltaInput(): ReplayTableSchemaDeltaInput {
  return { flowConnectionConfigs: undefined, tableSchemaDeltas: [] };
}

export const ReplayTableSchemaDeltaInput = {
  encode(message: ReplayTableSchemaDeltaInput, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowConnectionConfigs !== undefined) {
      FlowConnectionConfigs.encode(message.flowConnectionConfigs, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.tableSchemaDeltas) {
      TableSchemaDelta.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReplayTableSchemaDeltaInput {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReplayTableSchemaDeltaInput();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.flowConnectionConfigs = FlowConnectionConfigs.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.tableSchemaDeltas.push(TableSchemaDelta.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ReplayTableSchemaDeltaInput {
    return {
      flowConnectionConfigs: isSet(object.flowConnectionConfigs)
        ? FlowConnectionConfigs.fromJSON(object.flowConnectionConfigs)
        : undefined,
      tableSchemaDeltas: Array.isArray(object?.tableSchemaDeltas)
        ? object.tableSchemaDeltas.map((e: any) => TableSchemaDelta.fromJSON(e))
        : [],
    };
  },

  toJSON(message: ReplayTableSchemaDeltaInput): unknown {
    const obj: any = {};
    if (message.flowConnectionConfigs !== undefined) {
      obj.flowConnectionConfigs = FlowConnectionConfigs.toJSON(message.flowConnectionConfigs);
    }
    if (message.tableSchemaDeltas?.length) {
      obj.tableSchemaDeltas = message.tableSchemaDeltas.map((e) => TableSchemaDelta.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ReplayTableSchemaDeltaInput>, I>>(base?: I): ReplayTableSchemaDeltaInput {
    return ReplayTableSchemaDeltaInput.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ReplayTableSchemaDeltaInput>, I>>(object: I): ReplayTableSchemaDeltaInput {
    const message = createBaseReplayTableSchemaDeltaInput();
    message.flowConnectionConfigs =
      (object.flowConnectionConfigs !== undefined && object.flowConnectionConfigs !== null)
        ? FlowConnectionConfigs.fromPartial(object.flowConnectionConfigs)
        : undefined;
    message.tableSchemaDeltas = object.tableSchemaDeltas?.map((e) => TableSchemaDelta.fromPartial(e)) || [];
    return message;
  },
};

function createBaseQRepFlowState(): QRepFlowState {
  return { lastPartition: undefined, numPartitionsProcessed: 0, needsResync: false, disableWaitForNewRows: false };
}

export const QRepFlowState = {
  encode(message: QRepFlowState, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.lastPartition !== undefined) {
      QRepPartition.encode(message.lastPartition, writer.uint32(10).fork()).ldelim();
    }
    if (message.numPartitionsProcessed !== 0) {
      writer.uint32(16).uint64(message.numPartitionsProcessed);
    }
    if (message.needsResync === true) {
      writer.uint32(24).bool(message.needsResync);
    }
    if (message.disableWaitForNewRows === true) {
      writer.uint32(32).bool(message.disableWaitForNewRows);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepFlowState {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepFlowState();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.lastPartition = QRepPartition.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.numPartitionsProcessed = longToNumber(reader.uint64() as Long);
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.needsResync = reader.bool();
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.disableWaitForNewRows = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepFlowState {
    return {
      lastPartition: isSet(object.lastPartition) ? QRepPartition.fromJSON(object.lastPartition) : undefined,
      numPartitionsProcessed: isSet(object.numPartitionsProcessed) ? Number(object.numPartitionsProcessed) : 0,
      needsResync: isSet(object.needsResync) ? Boolean(object.needsResync) : false,
      disableWaitForNewRows: isSet(object.disableWaitForNewRows) ? Boolean(object.disableWaitForNewRows) : false,
    };
  },

  toJSON(message: QRepFlowState): unknown {
    const obj: any = {};
    if (message.lastPartition !== undefined) {
      obj.lastPartition = QRepPartition.toJSON(message.lastPartition);
    }
    if (message.numPartitionsProcessed !== 0) {
      obj.numPartitionsProcessed = Math.round(message.numPartitionsProcessed);
    }
    if (message.needsResync === true) {
      obj.needsResync = message.needsResync;
    }
    if (message.disableWaitForNewRows === true) {
      obj.disableWaitForNewRows = message.disableWaitForNewRows;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepFlowState>, I>>(base?: I): QRepFlowState {
    return QRepFlowState.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepFlowState>, I>>(object: I): QRepFlowState {
    const message = createBaseQRepFlowState();
    message.lastPartition = (object.lastPartition !== undefined && object.lastPartition !== null)
      ? QRepPartition.fromPartial(object.lastPartition)
      : undefined;
    message.numPartitionsProcessed = object.numPartitionsProcessed ?? 0;
    message.needsResync = object.needsResync ?? false;
    message.disableWaitForNewRows = object.disableWaitForNewRows ?? false;
    return message;
  },
};

declare const self: any | undefined;
declare const window: any | undefined;
declare const global: any | undefined;
const tsProtoGlobalThis: any = (() => {
  if (typeof globalThis !== "undefined") {
    return globalThis;
  }
  if (typeof self !== "undefined") {
    return self;
  }
  if (typeof window !== "undefined") {
    return window;
  }
  if (typeof global !== "undefined") {
    return global;
  }
  throw "Unable to locate global object";
})();

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function toTimestamp(date: Date): Timestamp {
  const seconds = date.getTime() / 1_000;
  const nanos = (date.getTime() % 1_000) * 1_000_000;
  return { seconds, nanos };
}

function fromTimestamp(t: Timestamp): Date {
  let millis = (t.seconds || 0) * 1_000;
  millis += (t.nanos || 0) / 1_000_000;
  return new Date(millis);
}

function fromJsonTimestamp(o: any): Date {
  if (o instanceof Date) {
    return o;
  } else if (typeof o === "string") {
    return new Date(o);
  } else {
    return fromTimestamp(Timestamp.fromJSON(o));
  }
}

function longToNumber(long: Long): number {
  if (long.gt(Number.MAX_SAFE_INTEGER)) {
    throw new tsProtoGlobalThis.Error("Value is larger than Number.MAX_SAFE_INTEGER");
  }
  return long.toNumber();
}

if (_m0.util.Long !== Long) {
  _m0.util.Long = Long as any;
  _m0.configure();
}

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
