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
  password?: string | undefined;
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
}

export interface EventHubConfig {
  namespace: string;
  resourceGroup: string;
  location: string;
  metadataDb: PostgresConfig | undefined;
}

export interface S3Config {
  url: string;
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
  return { host: "", port: 0, user: "", password: "", database: "", transactionSnapshot: "" };
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
    return message;
  },
};

function createBaseEventHubConfig(): EventHubConfig {
  return { namespace: "", resourceGroup: "", location: "", metadataDb: undefined };
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
    return message;
  },
};

function createBaseS3Config(): S3Config {
  return { url: "" };
}

export const S3Config = {
  encode(message: S3Config, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.url !== "") {
      writer.uint32(10).string(message.url);
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
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): S3Config {
    return { url: isSet(object.url) ? String(object.url) : "" };
  },

  toJSON(message: S3Config): unknown {
    const obj: any = {};
    if (message.url !== "") {
      obj.url = message.url;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<S3Config>, I>>(base?: I): S3Config {
    return S3Config.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<S3Config>, I>>(object: I): S3Config {
    const message = createBaseS3Config();
    message.url = object.url ?? "";
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
