/* eslint-disable */
import Long from "long";
import _m0 from "protobufjs/minimal";

export const protobufPackage = "peerdb_peers";

export enum DBType {
  BIGQUERY = 0,
  SNOWFLAKE = 1,
  MONGO = 2,
  POSTGRES = 3,
  EVENTHUB = 4,
  S3 = 5,
  SQLSERVER = 6,
  EVENTHUB_GROUP = 7,
  UNRECOGNIZED = -1,
}

export function dBTypeFromJSON(object: any): DBType {
  switch (object) {
    case 0:
    case "BIGQUERY":
      return DBType.BIGQUERY;
    case 1:
    case "SNOWFLAKE":
      return DBType.SNOWFLAKE;
    case 2:
    case "MONGO":
      return DBType.MONGO;
    case 3:
    case "POSTGRES":
      return DBType.POSTGRES;
    case 4:
    case "EVENTHUB":
      return DBType.EVENTHUB;
    case 5:
    case "S3":
      return DBType.S3;
    case 6:
    case "SQLSERVER":
      return DBType.SQLSERVER;
    case 7:
    case "EVENTHUB_GROUP":
      return DBType.EVENTHUB_GROUP;
    case -1:
    case "UNRECOGNIZED":
    default:
      return DBType.UNRECOGNIZED;
  }
}

export function dBTypeToJSON(object: DBType): string {
  switch (object) {
    case DBType.BIGQUERY:
      return "BIGQUERY";
    case DBType.SNOWFLAKE:
      return "SNOWFLAKE";
    case DBType.MONGO:
      return "MONGO";
    case DBType.POSTGRES:
      return "POSTGRES";
    case DBType.EVENTHUB:
      return "EVENTHUB";
    case DBType.S3:
      return "S3";
    case DBType.SQLSERVER:
      return "SQLSERVER";
    case DBType.EVENTHUB_GROUP:
      return "EVENTHUB_GROUP";
    case DBType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface SnowflakeConfig {
  accountId: string;
  username: string;
  privateKey: string;
  database: string;
  warehouse: string;
  role: string;
  queryTimeout: number;
  s3Integration: string;
  password?:
    | string
    | undefined;
  /** defaults to _PEERDB_INTERNAL */
  metadataSchema?: string | undefined;
}

export interface BigqueryConfig {
  authType: string;
  projectId: string;
  privateKeyId: string;
  privateKey: string;
  clientEmail: string;
  clientId: string;
  authUri: string;
  tokenUri: string;
  authProviderX509CertUrl: string;
  clientX509CertUrl: string;
  datasetId: string;
}

export interface MongoConfig {
  username: string;
  password: string;
  clusterurl: string;
  clusterport: number;
  database: string;
}

export interface PostgresConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  /** this is used only in query replication mode right now. */
  transactionSnapshot: string;
  /** defaults to _peerdb_internal */
  metadataSchema?: string | undefined;
}

export interface EventHubConfig {
  namespace: string;
  resourceGroup: string;
  location: string;
  metadataDb:
    | PostgresConfig
    | undefined;
  /** if this is empty PeerDB uses `AZURE_SUBSCRIPTION_ID` environment variable. */
  subscriptionId: string;
  /** defaults to 3 */
  partitionCount: number;
  /** defaults to 7 */
  messageRetentionInDays: number;
}

export interface EventHubGroupConfig {
  /** event hub peer name to event hub config */
  eventhubs: { [key: string]: EventHubConfig };
  metadataDb: PostgresConfig | undefined;
  unnestColumns: string[];
}

export interface EventHubGroupConfig_EventhubsEntry {
  key: string;
  value: EventHubConfig | undefined;
}

export interface S3Config {
  url: string;
  accessKeyId?: string | undefined;
  secretAccessKey?: string | undefined;
  roleArn?: string | undefined;
  region?: string | undefined;
  endpoint?: string | undefined;
  metadataDb: PostgresConfig | undefined;
}

export interface SqlServerConfig {
  server: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export interface Peer {
  name: string;
  type: DBType;
  snowflakeConfig?: SnowflakeConfig | undefined;
  bigqueryConfig?: BigqueryConfig | undefined;
  mongoConfig?: MongoConfig | undefined;
  postgresConfig?: PostgresConfig | undefined;
  eventhubConfig?: EventHubConfig | undefined;
  s3Config?: S3Config | undefined;
  sqlserverConfig?: SqlServerConfig | undefined;
  eventhubGroupConfig?: EventHubGroupConfig | undefined;
}

function createBaseSnowflakeConfig(): SnowflakeConfig {
  return {
    accountId: "",
    username: "",
    privateKey: "",
    database: "",
    warehouse: "",
    role: "",
    queryTimeout: 0,
    s3Integration: "",
    password: undefined,
    metadataSchema: undefined,
  };
}

export const SnowflakeConfig = {
  encode(message: SnowflakeConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.accountId !== "") {
      writer.uint32(10).string(message.accountId);
    }
    if (message.username !== "") {
      writer.uint32(18).string(message.username);
    }
    if (message.privateKey !== "") {
      writer.uint32(26).string(message.privateKey);
    }
    if (message.database !== "") {
      writer.uint32(34).string(message.database);
    }
    if (message.warehouse !== "") {
      writer.uint32(50).string(message.warehouse);
    }
    if (message.role !== "") {
      writer.uint32(58).string(message.role);
    }
    if (message.queryTimeout !== 0) {
      writer.uint32(64).uint64(message.queryTimeout);
    }
    if (message.s3Integration !== "") {
      writer.uint32(74).string(message.s3Integration);
    }
    if (message.password !== undefined) {
      writer.uint32(82).string(message.password);
    }
    if (message.metadataSchema !== undefined) {
      writer.uint32(90).string(message.metadataSchema);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SnowflakeConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSnowflakeConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.accountId = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.username = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.privateKey = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.database = reader.string();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.warehouse = reader.string();
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.role = reader.string();
          continue;
        case 8:
          if (tag !== 64) {
            break;
          }

          message.queryTimeout = longToNumber(reader.uint64() as Long);
          continue;
        case 9:
          if (tag !== 74) {
            break;
          }

          message.s3Integration = reader.string();
          continue;
        case 10:
          if (tag !== 82) {
            break;
          }

          message.password = reader.string();
          continue;
        case 11:
          if (tag !== 90) {
            break;
          }

          message.metadataSchema = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SnowflakeConfig {
    return {
      accountId: isSet(object.accountId) ? String(object.accountId) : "",
      username: isSet(object.username) ? String(object.username) : "",
      privateKey: isSet(object.privateKey) ? String(object.privateKey) : "",
      database: isSet(object.database) ? String(object.database) : "",
      warehouse: isSet(object.warehouse) ? String(object.warehouse) : "",
      role: isSet(object.role) ? String(object.role) : "",
      queryTimeout: isSet(object.queryTimeout) ? Number(object.queryTimeout) : 0,
      s3Integration: isSet(object.s3Integration) ? String(object.s3Integration) : "",
      password: isSet(object.password) ? String(object.password) : undefined,
      metadataSchema: isSet(object.metadataSchema) ? String(object.metadataSchema) : undefined,
    };
  },

  toJSON(message: SnowflakeConfig): unknown {
    const obj: any = {};
    if (message.accountId !== "") {
      obj.accountId = message.accountId;
    }
    if (message.username !== "") {
      obj.username = message.username;
    }
    if (message.privateKey !== "") {
      obj.privateKey = message.privateKey;
    }
    if (message.database !== "") {
      obj.database = message.database;
    }
    if (message.warehouse !== "") {
      obj.warehouse = message.warehouse;
    }
    if (message.role !== "") {
      obj.role = message.role;
    }
    if (message.queryTimeout !== 0) {
      obj.queryTimeout = Math.round(message.queryTimeout);
    }
    if (message.s3Integration !== "") {
      obj.s3Integration = message.s3Integration;
    }
    if (message.password !== undefined) {
      obj.password = message.password;
    }
    if (message.metadataSchema !== undefined) {
      obj.metadataSchema = message.metadataSchema;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SnowflakeConfig>, I>>(base?: I): SnowflakeConfig {
    return SnowflakeConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SnowflakeConfig>, I>>(object: I): SnowflakeConfig {
    const message = createBaseSnowflakeConfig();
    message.accountId = object.accountId ?? "";
    message.username = object.username ?? "";
    message.privateKey = object.privateKey ?? "";
    message.database = object.database ?? "";
    message.warehouse = object.warehouse ?? "";
    message.role = object.role ?? "";
    message.queryTimeout = object.queryTimeout ?? 0;
    message.s3Integration = object.s3Integration ?? "";
    message.password = object.password ?? undefined;
    message.metadataSchema = object.metadataSchema ?? undefined;
    return message;
  },
};

function createBaseBigqueryConfig(): BigqueryConfig {
  return {
    authType: "",
    projectId: "",
    privateKeyId: "",
    privateKey: "",
    clientEmail: "",
    clientId: "",
    authUri: "",
    tokenUri: "",
    authProviderX509CertUrl: "",
    clientX509CertUrl: "",
    datasetId: "",
  };
}

export const BigqueryConfig = {
  encode(message: BigqueryConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.authType !== "") {
      writer.uint32(10).string(message.authType);
    }
    if (message.projectId !== "") {
      writer.uint32(18).string(message.projectId);
    }
    if (message.privateKeyId !== "") {
      writer.uint32(26).string(message.privateKeyId);
    }
    if (message.privateKey !== "") {
      writer.uint32(34).string(message.privateKey);
    }
    if (message.clientEmail !== "") {
      writer.uint32(42).string(message.clientEmail);
    }
    if (message.clientId !== "") {
      writer.uint32(50).string(message.clientId);
    }
    if (message.authUri !== "") {
      writer.uint32(58).string(message.authUri);
    }
    if (message.tokenUri !== "") {
      writer.uint32(66).string(message.tokenUri);
    }
    if (message.authProviderX509CertUrl !== "") {
      writer.uint32(74).string(message.authProviderX509CertUrl);
    }
    if (message.clientX509CertUrl !== "") {
      writer.uint32(82).string(message.clientX509CertUrl);
    }
    if (message.datasetId !== "") {
      writer.uint32(90).string(message.datasetId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BigqueryConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBigqueryConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.authType = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.projectId = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.privateKeyId = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.privateKey = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.clientEmail = reader.string();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.clientId = reader.string();
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.authUri = reader.string();
          continue;
        case 8:
          if (tag !== 66) {
            break;
          }

          message.tokenUri = reader.string();
          continue;
        case 9:
          if (tag !== 74) {
            break;
          }

          message.authProviderX509CertUrl = reader.string();
          continue;
        case 10:
          if (tag !== 82) {
            break;
          }

          message.clientX509CertUrl = reader.string();
          continue;
        case 11:
          if (tag !== 90) {
            break;
          }

          message.datasetId = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): BigqueryConfig {
    return {
      authType: isSet(object.authType) ? String(object.authType) : "",
      projectId: isSet(object.projectId) ? String(object.projectId) : "",
      privateKeyId: isSet(object.privateKeyId) ? String(object.privateKeyId) : "",
      privateKey: isSet(object.privateKey) ? String(object.privateKey) : "",
      clientEmail: isSet(object.clientEmail) ? String(object.clientEmail) : "",
      clientId: isSet(object.clientId) ? String(object.clientId) : "",
      authUri: isSet(object.authUri) ? String(object.authUri) : "",
      tokenUri: isSet(object.tokenUri) ? String(object.tokenUri) : "",
      authProviderX509CertUrl: isSet(object.authProviderX509CertUrl) ? String(object.authProviderX509CertUrl) : "",
      clientX509CertUrl: isSet(object.clientX509CertUrl) ? String(object.clientX509CertUrl) : "",
      datasetId: isSet(object.datasetId) ? String(object.datasetId) : "",
    };
  },

  toJSON(message: BigqueryConfig): unknown {
    const obj: any = {};
    if (message.authType !== "") {
      obj.authType = message.authType;
    }
    if (message.projectId !== "") {
      obj.projectId = message.projectId;
    }
    if (message.privateKeyId !== "") {
      obj.privateKeyId = message.privateKeyId;
    }
    if (message.privateKey !== "") {
      obj.privateKey = message.privateKey;
    }
    if (message.clientEmail !== "") {
      obj.clientEmail = message.clientEmail;
    }
    if (message.clientId !== "") {
      obj.clientId = message.clientId;
    }
    if (message.authUri !== "") {
      obj.authUri = message.authUri;
    }
    if (message.tokenUri !== "") {
      obj.tokenUri = message.tokenUri;
    }
    if (message.authProviderX509CertUrl !== "") {
      obj.authProviderX509CertUrl = message.authProviderX509CertUrl;
    }
    if (message.clientX509CertUrl !== "") {
      obj.clientX509CertUrl = message.clientX509CertUrl;
    }
    if (message.datasetId !== "") {
      obj.datasetId = message.datasetId;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<BigqueryConfig>, I>>(base?: I): BigqueryConfig {
    return BigqueryConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<BigqueryConfig>, I>>(object: I): BigqueryConfig {
    const message = createBaseBigqueryConfig();
    message.authType = object.authType ?? "";
    message.projectId = object.projectId ?? "";
    message.privateKeyId = object.privateKeyId ?? "";
    message.privateKey = object.privateKey ?? "";
    message.clientEmail = object.clientEmail ?? "";
    message.clientId = object.clientId ?? "";
    message.authUri = object.authUri ?? "";
    message.tokenUri = object.tokenUri ?? "";
    message.authProviderX509CertUrl = object.authProviderX509CertUrl ?? "";
    message.clientX509CertUrl = object.clientX509CertUrl ?? "";
    message.datasetId = object.datasetId ?? "";
    return message;
  },
};

function createBaseMongoConfig(): MongoConfig {
  return { username: "", password: "", clusterurl: "", clusterport: 0, database: "" };
}

export const MongoConfig = {
  encode(message: MongoConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.username !== "") {
      writer.uint32(10).string(message.username);
    }
    if (message.password !== "") {
      writer.uint32(18).string(message.password);
    }
    if (message.clusterurl !== "") {
      writer.uint32(26).string(message.clusterurl);
    }
    if (message.clusterport !== 0) {
      writer.uint32(32).int32(message.clusterport);
    }
    if (message.database !== "") {
      writer.uint32(42).string(message.database);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MongoConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMongoConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.username = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.password = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.clusterurl = reader.string();
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.clusterport = reader.int32();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.database = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): MongoConfig {
    return {
      username: isSet(object.username) ? String(object.username) : "",
      password: isSet(object.password) ? String(object.password) : "",
      clusterurl: isSet(object.clusterurl) ? String(object.clusterurl) : "",
      clusterport: isSet(object.clusterport) ? Number(object.clusterport) : 0,
      database: isSet(object.database) ? String(object.database) : "",
    };
  },

  toJSON(message: MongoConfig): unknown {
    const obj: any = {};
    if (message.username !== "") {
      obj.username = message.username;
    }
    if (message.password !== "") {
      obj.password = message.password;
    }
    if (message.clusterurl !== "") {
      obj.clusterurl = message.clusterurl;
    }
    if (message.clusterport !== 0) {
      obj.clusterport = Math.round(message.clusterport);
    }
    if (message.database !== "") {
      obj.database = message.database;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<MongoConfig>, I>>(base?: I): MongoConfig {
    return MongoConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<MongoConfig>, I>>(object: I): MongoConfig {
    const message = createBaseMongoConfig();
    message.username = object.username ?? "";
    message.password = object.password ?? "";
    message.clusterurl = object.clusterurl ?? "";
    message.clusterport = object.clusterport ?? 0;
    message.database = object.database ?? "";
    return message;
  },
};

function createBasePostgresConfig(): PostgresConfig {
  return {
    host: "",
    port: 0,
    user: "",
    password: "",
    database: "",
    transactionSnapshot: "",
    metadataSchema: undefined,
  };
}

export const PostgresConfig = {
  encode(message: PostgresConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.host !== "") {
      writer.uint32(10).string(message.host);
    }
    if (message.port !== 0) {
      writer.uint32(16).uint32(message.port);
    }
    if (message.user !== "") {
      writer.uint32(26).string(message.user);
    }
    if (message.password !== "") {
      writer.uint32(34).string(message.password);
    }
    if (message.database !== "") {
      writer.uint32(42).string(message.database);
    }
    if (message.transactionSnapshot !== "") {
      writer.uint32(50).string(message.transactionSnapshot);
    }
    if (message.metadataSchema !== undefined) {
      writer.uint32(58).string(message.metadataSchema);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PostgresConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePostgresConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.host = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.port = reader.uint32();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.user = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.password = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.database = reader.string();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.transactionSnapshot = reader.string();
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.metadataSchema = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PostgresConfig {
    return {
      host: isSet(object.host) ? String(object.host) : "",
      port: isSet(object.port) ? Number(object.port) : 0,
      user: isSet(object.user) ? String(object.user) : "",
      password: isSet(object.password) ? String(object.password) : "",
      database: isSet(object.database) ? String(object.database) : "",
      transactionSnapshot: isSet(object.transactionSnapshot) ? String(object.transactionSnapshot) : "",
      metadataSchema: isSet(object.metadataSchema) ? String(object.metadataSchema) : undefined,
    };
  },

  toJSON(message: PostgresConfig): unknown {
    const obj: any = {};
    if (message.host !== "") {
      obj.host = message.host;
    }
    if (message.port !== 0) {
      obj.port = Math.round(message.port);
    }
    if (message.user !== "") {
      obj.user = message.user;
    }
    if (message.password !== "") {
      obj.password = message.password;
    }
    if (message.database !== "") {
      obj.database = message.database;
    }
    if (message.transactionSnapshot !== "") {
      obj.transactionSnapshot = message.transactionSnapshot;
    }
    if (message.metadataSchema !== undefined) {
      obj.metadataSchema = message.metadataSchema;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PostgresConfig>, I>>(base?: I): PostgresConfig {
    return PostgresConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PostgresConfig>, I>>(object: I): PostgresConfig {
    const message = createBasePostgresConfig();
    message.host = object.host ?? "";
    message.port = object.port ?? 0;
    message.user = object.user ?? "";
    message.password = object.password ?? "";
    message.database = object.database ?? "";
    message.transactionSnapshot = object.transactionSnapshot ?? "";
    message.metadataSchema = object.metadataSchema ?? undefined;
    return message;
  },
};

function createBaseEventHubConfig(): EventHubConfig {
  return {
    namespace: "",
    resourceGroup: "",
    location: "",
    metadataDb: undefined,
    subscriptionId: "",
    partitionCount: 0,
    messageRetentionInDays: 0,
  };
}

export const EventHubConfig = {
  encode(message: EventHubConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.namespace !== "") {
      writer.uint32(10).string(message.namespace);
    }
    if (message.resourceGroup !== "") {
      writer.uint32(18).string(message.resourceGroup);
    }
    if (message.location !== "") {
      writer.uint32(26).string(message.location);
    }
    if (message.metadataDb !== undefined) {
      PostgresConfig.encode(message.metadataDb, writer.uint32(34).fork()).ldelim();
    }
    if (message.subscriptionId !== "") {
      writer.uint32(42).string(message.subscriptionId);
    }
    if (message.partitionCount !== 0) {
      writer.uint32(48).uint32(message.partitionCount);
    }
    if (message.messageRetentionInDays !== 0) {
      writer.uint32(56).uint32(message.messageRetentionInDays);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EventHubConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEventHubConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.namespace = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.resourceGroup = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.location = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.metadataDb = PostgresConfig.decode(reader, reader.uint32());
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.subscriptionId = reader.string();
          continue;
        case 6:
          if (tag !== 48) {
            break;
          }

          message.partitionCount = reader.uint32();
          continue;
        case 7:
          if (tag !== 56) {
            break;
          }

          message.messageRetentionInDays = reader.uint32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EventHubConfig {
    return {
      namespace: isSet(object.namespace) ? String(object.namespace) : "",
      resourceGroup: isSet(object.resourceGroup) ? String(object.resourceGroup) : "",
      location: isSet(object.location) ? String(object.location) : "",
      metadataDb: isSet(object.metadataDb) ? PostgresConfig.fromJSON(object.metadataDb) : undefined,
      subscriptionId: isSet(object.subscriptionId) ? String(object.subscriptionId) : "",
      partitionCount: isSet(object.partitionCount) ? Number(object.partitionCount) : 0,
      messageRetentionInDays: isSet(object.messageRetentionInDays) ? Number(object.messageRetentionInDays) : 0,
    };
  },

  toJSON(message: EventHubConfig): unknown {
    const obj: any = {};
    if (message.namespace !== "") {
      obj.namespace = message.namespace;
    }
    if (message.resourceGroup !== "") {
      obj.resourceGroup = message.resourceGroup;
    }
    if (message.location !== "") {
      obj.location = message.location;
    }
    if (message.metadataDb !== undefined) {
      obj.metadataDb = PostgresConfig.toJSON(message.metadataDb);
    }
    if (message.subscriptionId !== "") {
      obj.subscriptionId = message.subscriptionId;
    }
    if (message.partitionCount !== 0) {
      obj.partitionCount = Math.round(message.partitionCount);
    }
    if (message.messageRetentionInDays !== 0) {
      obj.messageRetentionInDays = Math.round(message.messageRetentionInDays);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EventHubConfig>, I>>(base?: I): EventHubConfig {
    return EventHubConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EventHubConfig>, I>>(object: I): EventHubConfig {
    const message = createBaseEventHubConfig();
    message.namespace = object.namespace ?? "";
    message.resourceGroup = object.resourceGroup ?? "";
    message.location = object.location ?? "";
    message.metadataDb = (object.metadataDb !== undefined && object.metadataDb !== null)
      ? PostgresConfig.fromPartial(object.metadataDb)
      : undefined;
    message.subscriptionId = object.subscriptionId ?? "";
    message.partitionCount = object.partitionCount ?? 0;
    message.messageRetentionInDays = object.messageRetentionInDays ?? 0;
    return message;
  },
};

function createBaseEventHubGroupConfig(): EventHubGroupConfig {
  return { eventhubs: {}, metadataDb: undefined, unnestColumns: [] };
}

export const EventHubGroupConfig = {
  encode(message: EventHubGroupConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.eventhubs).forEach(([key, value]) => {
      EventHubGroupConfig_EventhubsEntry.encode({ key: key as any, value }, writer.uint32(10).fork()).ldelim();
    });
    if (message.metadataDb !== undefined) {
      PostgresConfig.encode(message.metadataDb, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.unnestColumns) {
      writer.uint32(26).string(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EventHubGroupConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEventHubGroupConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          const entry1 = EventHubGroupConfig_EventhubsEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.eventhubs[entry1.key] = entry1.value;
          }
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.metadataDb = PostgresConfig.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.unnestColumns.push(reader.string());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EventHubGroupConfig {
    return {
      eventhubs: isObject(object.eventhubs)
        ? Object.entries(object.eventhubs).reduce<{ [key: string]: EventHubConfig }>((acc, [key, value]) => {
          acc[key] = EventHubConfig.fromJSON(value);
          return acc;
        }, {})
        : {},
      metadataDb: isSet(object.metadataDb) ? PostgresConfig.fromJSON(object.metadataDb) : undefined,
      unnestColumns: Array.isArray(object?.unnestColumns) ? object.unnestColumns.map((e: any) => String(e)) : [],
    };
  },

  toJSON(message: EventHubGroupConfig): unknown {
    const obj: any = {};
    if (message.eventhubs) {
      const entries = Object.entries(message.eventhubs);
      if (entries.length > 0) {
        obj.eventhubs = {};
        entries.forEach(([k, v]) => {
          obj.eventhubs[k] = EventHubConfig.toJSON(v);
        });
      }
    }
    if (message.metadataDb !== undefined) {
      obj.metadataDb = PostgresConfig.toJSON(message.metadataDb);
    }
    if (message.unnestColumns?.length) {
      obj.unnestColumns = message.unnestColumns;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EventHubGroupConfig>, I>>(base?: I): EventHubGroupConfig {
    return EventHubGroupConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EventHubGroupConfig>, I>>(object: I): EventHubGroupConfig {
    const message = createBaseEventHubGroupConfig();
    message.eventhubs = Object.entries(object.eventhubs ?? {}).reduce<{ [key: string]: EventHubConfig }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = EventHubConfig.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.metadataDb = (object.metadataDb !== undefined && object.metadataDb !== null)
      ? PostgresConfig.fromPartial(object.metadataDb)
      : undefined;
    message.unnestColumns = object.unnestColumns?.map((e) => e) || [];
    return message;
  },
};

function createBaseEventHubGroupConfig_EventhubsEntry(): EventHubGroupConfig_EventhubsEntry {
  return { key: "", value: undefined };
}

export const EventHubGroupConfig_EventhubsEntry = {
  encode(message: EventHubGroupConfig_EventhubsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== undefined) {
      EventHubConfig.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): EventHubGroupConfig_EventhubsEntry {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseEventHubGroupConfig_EventhubsEntry();
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

          message.value = EventHubConfig.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): EventHubGroupConfig_EventhubsEntry {
    return {
      key: isSet(object.key) ? String(object.key) : "",
      value: isSet(object.value) ? EventHubConfig.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: EventHubGroupConfig_EventhubsEntry): unknown {
    const obj: any = {};
    if (message.key !== "") {
      obj.key = message.key;
    }
    if (message.value !== undefined) {
      obj.value = EventHubConfig.toJSON(message.value);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<EventHubGroupConfig_EventhubsEntry>, I>>(
    base?: I,
  ): EventHubGroupConfig_EventhubsEntry {
    return EventHubGroupConfig_EventhubsEntry.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<EventHubGroupConfig_EventhubsEntry>, I>>(
    object: I,
  ): EventHubGroupConfig_EventhubsEntry {
    const message = createBaseEventHubGroupConfig_EventhubsEntry();
    message.key = object.key ?? "";
    message.value = (object.value !== undefined && object.value !== null)
      ? EventHubConfig.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseS3Config(): S3Config {
  return {
    url: "",
    accessKeyId: undefined,
    secretAccessKey: undefined,
    roleArn: undefined,
    region: undefined,
    endpoint: undefined,
    metadataDb: undefined,
  };
}

export const S3Config = {
  encode(message: S3Config, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.url !== "") {
      writer.uint32(10).string(message.url);
    }
    if (message.accessKeyId !== undefined) {
      writer.uint32(18).string(message.accessKeyId);
    }
    if (message.secretAccessKey !== undefined) {
      writer.uint32(26).string(message.secretAccessKey);
    }
    if (message.roleArn !== undefined) {
      writer.uint32(34).string(message.roleArn);
    }
    if (message.region !== undefined) {
      writer.uint32(42).string(message.region);
    }
    if (message.endpoint !== undefined) {
      writer.uint32(50).string(message.endpoint);
    }
    if (message.metadataDb !== undefined) {
      PostgresConfig.encode(message.metadataDb, writer.uint32(58).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): S3Config {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseS3Config();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.url = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.accessKeyId = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.secretAccessKey = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.roleArn = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.region = reader.string();
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.endpoint = reader.string();
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.metadataDb = PostgresConfig.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): S3Config {
    return {
      url: isSet(object.url) ? String(object.url) : "",
      accessKeyId: isSet(object.accessKeyId) ? String(object.accessKeyId) : undefined,
      secretAccessKey: isSet(object.secretAccessKey) ? String(object.secretAccessKey) : undefined,
      roleArn: isSet(object.roleArn) ? String(object.roleArn) : undefined,
      region: isSet(object.region) ? String(object.region) : undefined,
      endpoint: isSet(object.endpoint) ? String(object.endpoint) : undefined,
      metadataDb: isSet(object.metadataDb) ? PostgresConfig.fromJSON(object.metadataDb) : undefined,
    };
  },

  toJSON(message: S3Config): unknown {
    const obj: any = {};
    if (message.url !== "") {
      obj.url = message.url;
    }
    if (message.accessKeyId !== undefined) {
      obj.accessKeyId = message.accessKeyId;
    }
    if (message.secretAccessKey !== undefined) {
      obj.secretAccessKey = message.secretAccessKey;
    }
    if (message.roleArn !== undefined) {
      obj.roleArn = message.roleArn;
    }
    if (message.region !== undefined) {
      obj.region = message.region;
    }
    if (message.endpoint !== undefined) {
      obj.endpoint = message.endpoint;
    }
    if (message.metadataDb !== undefined) {
      obj.metadataDb = PostgresConfig.toJSON(message.metadataDb);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<S3Config>, I>>(base?: I): S3Config {
    return S3Config.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<S3Config>, I>>(object: I): S3Config {
    const message = createBaseS3Config();
    message.url = object.url ?? "";
    message.accessKeyId = object.accessKeyId ?? undefined;
    message.secretAccessKey = object.secretAccessKey ?? undefined;
    message.roleArn = object.roleArn ?? undefined;
    message.region = object.region ?? undefined;
    message.endpoint = object.endpoint ?? undefined;
    message.metadataDb = (object.metadataDb !== undefined && object.metadataDb !== null)
      ? PostgresConfig.fromPartial(object.metadataDb)
      : undefined;
    return message;
  },
};

function createBaseSqlServerConfig(): SqlServerConfig {
  return { server: "", port: 0, user: "", password: "", database: "" };
}

export const SqlServerConfig = {
  encode(message: SqlServerConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.server !== "") {
      writer.uint32(10).string(message.server);
    }
    if (message.port !== 0) {
      writer.uint32(16).uint32(message.port);
    }
    if (message.user !== "") {
      writer.uint32(26).string(message.user);
    }
    if (message.password !== "") {
      writer.uint32(34).string(message.password);
    }
    if (message.database !== "") {
      writer.uint32(42).string(message.database);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SqlServerConfig {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSqlServerConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.server = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.port = reader.uint32();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.user = reader.string();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.password = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.database = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SqlServerConfig {
    return {
      server: isSet(object.server) ? String(object.server) : "",
      port: isSet(object.port) ? Number(object.port) : 0,
      user: isSet(object.user) ? String(object.user) : "",
      password: isSet(object.password) ? String(object.password) : "",
      database: isSet(object.database) ? String(object.database) : "",
    };
  },

  toJSON(message: SqlServerConfig): unknown {
    const obj: any = {};
    if (message.server !== "") {
      obj.server = message.server;
    }
    if (message.port !== 0) {
      obj.port = Math.round(message.port);
    }
    if (message.user !== "") {
      obj.user = message.user;
    }
    if (message.password !== "") {
      obj.password = message.password;
    }
    if (message.database !== "") {
      obj.database = message.database;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SqlServerConfig>, I>>(base?: I): SqlServerConfig {
    return SqlServerConfig.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SqlServerConfig>, I>>(object: I): SqlServerConfig {
    const message = createBaseSqlServerConfig();
    message.server = object.server ?? "";
    message.port = object.port ?? 0;
    message.user = object.user ?? "";
    message.password = object.password ?? "";
    message.database = object.database ?? "";
    return message;
  },
};

function createBasePeer(): Peer {
  return {
    name: "",
    type: 0,
    snowflakeConfig: undefined,
    bigqueryConfig: undefined,
    mongoConfig: undefined,
    postgresConfig: undefined,
    eventhubConfig: undefined,
    s3Config: undefined,
    sqlserverConfig: undefined,
    eventhubGroupConfig: undefined,
  };
}

export const Peer = {
  encode(message: Peer, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.name !== "") {
      writer.uint32(10).string(message.name);
    }
    if (message.type !== 0) {
      writer.uint32(16).int32(message.type);
    }
    if (message.snowflakeConfig !== undefined) {
      SnowflakeConfig.encode(message.snowflakeConfig, writer.uint32(26).fork()).ldelim();
    }
    if (message.bigqueryConfig !== undefined) {
      BigqueryConfig.encode(message.bigqueryConfig, writer.uint32(34).fork()).ldelim();
    }
    if (message.mongoConfig !== undefined) {
      MongoConfig.encode(message.mongoConfig, writer.uint32(42).fork()).ldelim();
    }
    if (message.postgresConfig !== undefined) {
      PostgresConfig.encode(message.postgresConfig, writer.uint32(50).fork()).ldelim();
    }
    if (message.eventhubConfig !== undefined) {
      EventHubConfig.encode(message.eventhubConfig, writer.uint32(58).fork()).ldelim();
    }
    if (message.s3Config !== undefined) {
      S3Config.encode(message.s3Config, writer.uint32(66).fork()).ldelim();
    }
    if (message.sqlserverConfig !== undefined) {
      SqlServerConfig.encode(message.sqlserverConfig, writer.uint32(74).fork()).ldelim();
    }
    if (message.eventhubGroupConfig !== undefined) {
      EventHubGroupConfig.encode(message.eventhubGroupConfig, writer.uint32(82).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Peer {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePeer();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.name = reader.string();
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.type = reader.int32() as any;
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.snowflakeConfig = SnowflakeConfig.decode(reader, reader.uint32());
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.bigqueryConfig = BigqueryConfig.decode(reader, reader.uint32());
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.mongoConfig = MongoConfig.decode(reader, reader.uint32());
          continue;
        case 6:
          if (tag !== 50) {
            break;
          }

          message.postgresConfig = PostgresConfig.decode(reader, reader.uint32());
          continue;
        case 7:
          if (tag !== 58) {
            break;
          }

          message.eventhubConfig = EventHubConfig.decode(reader, reader.uint32());
          continue;
        case 8:
          if (tag !== 66) {
            break;
          }

          message.s3Config = S3Config.decode(reader, reader.uint32());
          continue;
        case 9:
          if (tag !== 74) {
            break;
          }

          message.sqlserverConfig = SqlServerConfig.decode(reader, reader.uint32());
          continue;
        case 10:
          if (tag !== 82) {
            break;
          }

          message.eventhubGroupConfig = EventHubGroupConfig.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): Peer {
    return {
      name: isSet(object.name) ? String(object.name) : "",
      type: isSet(object.type) ? dBTypeFromJSON(object.type) : 0,
      snowflakeConfig: isSet(object.snowflakeConfig) ? SnowflakeConfig.fromJSON(object.snowflakeConfig) : undefined,
      bigqueryConfig: isSet(object.bigqueryConfig) ? BigqueryConfig.fromJSON(object.bigqueryConfig) : undefined,
      mongoConfig: isSet(object.mongoConfig) ? MongoConfig.fromJSON(object.mongoConfig) : undefined,
      postgresConfig: isSet(object.postgresConfig) ? PostgresConfig.fromJSON(object.postgresConfig) : undefined,
      eventhubConfig: isSet(object.eventhubConfig) ? EventHubConfig.fromJSON(object.eventhubConfig) : undefined,
      s3Config: isSet(object.s3Config) ? S3Config.fromJSON(object.s3Config) : undefined,
      sqlserverConfig: isSet(object.sqlserverConfig) ? SqlServerConfig.fromJSON(object.sqlserverConfig) : undefined,
      eventhubGroupConfig: isSet(object.eventhubGroupConfig)
        ? EventHubGroupConfig.fromJSON(object.eventhubGroupConfig)
        : undefined,
    };
  },

  toJSON(message: Peer): unknown {
    const obj: any = {};
    if (message.name !== "") {
      obj.name = message.name;
    }
    if (message.type !== 0) {
      obj.type = dBTypeToJSON(message.type);
    }
    if (message.snowflakeConfig !== undefined) {
      obj.snowflakeConfig = SnowflakeConfig.toJSON(message.snowflakeConfig);
    }
    if (message.bigqueryConfig !== undefined) {
      obj.bigqueryConfig = BigqueryConfig.toJSON(message.bigqueryConfig);
    }
    if (message.mongoConfig !== undefined) {
      obj.mongoConfig = MongoConfig.toJSON(message.mongoConfig);
    }
    if (message.postgresConfig !== undefined) {
      obj.postgresConfig = PostgresConfig.toJSON(message.postgresConfig);
    }
    if (message.eventhubConfig !== undefined) {
      obj.eventhubConfig = EventHubConfig.toJSON(message.eventhubConfig);
    }
    if (message.s3Config !== undefined) {
      obj.s3Config = S3Config.toJSON(message.s3Config);
    }
    if (message.sqlserverConfig !== undefined) {
      obj.sqlserverConfig = SqlServerConfig.toJSON(message.sqlserverConfig);
    }
    if (message.eventhubGroupConfig !== undefined) {
      obj.eventhubGroupConfig = EventHubGroupConfig.toJSON(message.eventhubGroupConfig);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<Peer>, I>>(base?: I): Peer {
    return Peer.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Peer>, I>>(object: I): Peer {
    const message = createBasePeer();
    message.name = object.name ?? "";
    message.type = object.type ?? 0;
    message.snowflakeConfig = (object.snowflakeConfig !== undefined && object.snowflakeConfig !== null)
      ? SnowflakeConfig.fromPartial(object.snowflakeConfig)
      : undefined;
    message.bigqueryConfig = (object.bigqueryConfig !== undefined && object.bigqueryConfig !== null)
      ? BigqueryConfig.fromPartial(object.bigqueryConfig)
      : undefined;
    message.mongoConfig = (object.mongoConfig !== undefined && object.mongoConfig !== null)
      ? MongoConfig.fromPartial(object.mongoConfig)
      : undefined;
    message.postgresConfig = (object.postgresConfig !== undefined && object.postgresConfig !== null)
      ? PostgresConfig.fromPartial(object.postgresConfig)
      : undefined;
    message.eventhubConfig = (object.eventhubConfig !== undefined && object.eventhubConfig !== null)
      ? EventHubConfig.fromPartial(object.eventhubConfig)
      : undefined;
    message.s3Config = (object.s3Config !== undefined && object.s3Config !== null)
      ? S3Config.fromPartial(object.s3Config)
      : undefined;
    message.sqlserverConfig = (object.sqlserverConfig !== undefined && object.sqlserverConfig !== null)
      ? SqlServerConfig.fromPartial(object.sqlserverConfig)
      : undefined;
    message.eventhubGroupConfig = (object.eventhubGroupConfig !== undefined && object.eventhubGroupConfig !== null)
      ? EventHubGroupConfig.fromPartial(object.eventhubGroupConfig)
      : undefined;
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
