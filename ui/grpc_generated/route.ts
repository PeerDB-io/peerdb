/* eslint-disable */
import {
  CallOptions,
  ChannelCredentials,
  Client,
  ClientOptions,
  ClientUnaryCall,
  handleUnaryCall,
  makeGenericClientConstructor,
  Metadata,
  ServiceError,
  UntypedServiceImplementation,
} from "@grpc/grpc-js";
import Long from "long";
import _m0 from "protobufjs/minimal";
import { FlowConnectionConfigs, QRepConfig } from "./flow";
import { Timestamp } from "./google/protobuf/timestamp";
import { Peer } from "./peers";

export const protobufPackage = "peerdb_route";

export enum ValidatePeerStatus {
  CREATION_UNKNOWN = 0,
  VALID = 1,
  INVALID = 2,
  UNRECOGNIZED = -1,
}

export function validatePeerStatusFromJSON(object: any): ValidatePeerStatus {
  switch (object) {
    case 0:
    case "CREATION_UNKNOWN":
      return ValidatePeerStatus.CREATION_UNKNOWN;
    case 1:
    case "VALID":
      return ValidatePeerStatus.VALID;
    case 2:
    case "INVALID":
      return ValidatePeerStatus.INVALID;
    case -1:
    case "UNRECOGNIZED":
    default:
      return ValidatePeerStatus.UNRECOGNIZED;
  }
}

export function validatePeerStatusToJSON(object: ValidatePeerStatus): string {
  switch (object) {
    case ValidatePeerStatus.CREATION_UNKNOWN:
      return "CREATION_UNKNOWN";
    case ValidatePeerStatus.VALID:
      return "VALID";
    case ValidatePeerStatus.INVALID:
      return "INVALID";
    case ValidatePeerStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export enum CreatePeerStatus {
  VALIDATION_UNKNOWN = 0,
  CREATED = 1,
  FAILED = 2,
  UNRECOGNIZED = -1,
}

export function createPeerStatusFromJSON(object: any): CreatePeerStatus {
  switch (object) {
    case 0:
    case "VALIDATION_UNKNOWN":
      return CreatePeerStatus.VALIDATION_UNKNOWN;
    case 1:
    case "CREATED":
      return CreatePeerStatus.CREATED;
    case 2:
    case "FAILED":
      return CreatePeerStatus.FAILED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return CreatePeerStatus.UNRECOGNIZED;
  }
}

export function createPeerStatusToJSON(object: CreatePeerStatus): string {
  switch (object) {
    case CreatePeerStatus.VALIDATION_UNKNOWN:
      return "VALIDATION_UNKNOWN";
    case CreatePeerStatus.CREATED:
      return "CREATED";
    case CreatePeerStatus.FAILED:
      return "FAILED";
    case CreatePeerStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface CreateCDCFlowRequest {
  connectionConfigs: FlowConnectionConfigs | undefined;
  createCatalogEntry: boolean;
}

export interface CreateCDCFlowResponse {
  worflowId: string;
}

export interface CreateQRepFlowRequest {
  qrepConfig: QRepConfig | undefined;
  createCatalogEntry: boolean;
}

export interface CreateQRepFlowResponse {
  worflowId: string;
}

export interface ShutdownRequest {
  workflowId: string;
  flowJobName: string;
  sourcePeer: Peer | undefined;
  destinationPeer: Peer | undefined;
}

export interface ShutdownResponse {
  ok: boolean;
  errorMessage: string;
}

export interface ValidatePeerRequest {
  peer: Peer | undefined;
}

export interface CreatePeerRequest {
  peer: Peer | undefined;
}

export interface ValidatePeerResponse {
  status: ValidatePeerStatus;
  message: string;
}

export interface CreatePeerResponse {
  status: CreatePeerStatus;
  message: string;
}

export interface MirrorStatusRequest {
  flowJobName: string;
}

export interface PartitionStatus {
  partitionId: string;
  startTime: Date | undefined;
  endTime: Date | undefined;
  numRows: number;
}

export interface QRepMirrorStatus {
  config:
    | QRepConfig
    | undefined;
  /**
   * TODO make note to see if we are still in initial copy
   * or if we are in the continuous streaming mode.
   */
  partitions: PartitionStatus[];
}

export interface CDCSyncStatus {
  startLsn: number;
  endLsn: number;
  numRows: number;
  startTime: Date | undefined;
  endTime: Date | undefined;
}

export interface PeerDataRequest {
  peerName: string;
}

export interface SlotInfo {
  slotName: string;
  redoLSN: string;
  restartLSN: string;
  lagInMb: number;
}

export interface StatInfo {
  pid: number;
  query: string;
  duration: number;
}

export interface PeerSlotResponse {
  slotData: SlotInfo[];
}

export interface PeerStatResponse {
  statData: StatInfo[];
}

export interface SnapshotStatus {
  clones: QRepMirrorStatus[];
}

export interface CDCMirrorStatus {
  config: FlowConnectionConfigs | undefined;
  snapshotStatus: SnapshotStatus | undefined;
  cdcSyncs: CDCSyncStatus[];
}

export interface MirrorStatusResponse {
  flowJobName: string;
  qrepStatus?: QRepMirrorStatus | undefined;
  cdcStatus?: CDCMirrorStatus | undefined;
  errorMessage: string;
}

function createBaseCreateCDCFlowRequest(): CreateCDCFlowRequest {
  return { connectionConfigs: undefined, createCatalogEntry: false };
}

export const CreateCDCFlowRequest = {
  encode(message: CreateCDCFlowRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.connectionConfigs !== undefined) {
      FlowConnectionConfigs.encode(message.connectionConfigs, writer.uint32(10).fork()).ldelim();
    }
    if (message.createCatalogEntry === true) {
      writer.uint32(16).bool(message.createCatalogEntry);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateCDCFlowRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateCDCFlowRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.connectionConfigs = FlowConnectionConfigs.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.createCatalogEntry = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateCDCFlowRequest {
    return {
      connectionConfigs: isSet(object.connectionConfigs)
        ? FlowConnectionConfigs.fromJSON(object.connectionConfigs)
        : undefined,
      createCatalogEntry: isSet(object.createCatalogEntry) ? Boolean(object.createCatalogEntry) : false,
    };
  },

  toJSON(message: CreateCDCFlowRequest): unknown {
    const obj: any = {};
    if (message.connectionConfigs !== undefined) {
      obj.connectionConfigs = FlowConnectionConfigs.toJSON(message.connectionConfigs);
    }
    if (message.createCatalogEntry === true) {
      obj.createCatalogEntry = message.createCatalogEntry;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateCDCFlowRequest>, I>>(base?: I): CreateCDCFlowRequest {
    return CreateCDCFlowRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateCDCFlowRequest>, I>>(object: I): CreateCDCFlowRequest {
    const message = createBaseCreateCDCFlowRequest();
    message.connectionConfigs = (object.connectionConfigs !== undefined && object.connectionConfigs !== null)
      ? FlowConnectionConfigs.fromPartial(object.connectionConfigs)
      : undefined;
    message.createCatalogEntry = object.createCatalogEntry ?? false;
    return message;
  },
};

function createBaseCreateCDCFlowResponse(): CreateCDCFlowResponse {
  return { worflowId: "" };
}

export const CreateCDCFlowResponse = {
  encode(message: CreateCDCFlowResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.worflowId !== "") {
      writer.uint32(10).string(message.worflowId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateCDCFlowResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateCDCFlowResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.worflowId = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateCDCFlowResponse {
    return { worflowId: isSet(object.worflowId) ? String(object.worflowId) : "" };
  },

  toJSON(message: CreateCDCFlowResponse): unknown {
    const obj: any = {};
    if (message.worflowId !== "") {
      obj.worflowId = message.worflowId;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateCDCFlowResponse>, I>>(base?: I): CreateCDCFlowResponse {
    return CreateCDCFlowResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateCDCFlowResponse>, I>>(object: I): CreateCDCFlowResponse {
    const message = createBaseCreateCDCFlowResponse();
    message.worflowId = object.worflowId ?? "";
    return message;
  },
};

function createBaseCreateQRepFlowRequest(): CreateQRepFlowRequest {
  return { qrepConfig: undefined, createCatalogEntry: false };
}

export const CreateQRepFlowRequest = {
  encode(message: CreateQRepFlowRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.qrepConfig !== undefined) {
      QRepConfig.encode(message.qrepConfig, writer.uint32(10).fork()).ldelim();
    }
    if (message.createCatalogEntry === true) {
      writer.uint32(16).bool(message.createCatalogEntry);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateQRepFlowRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateQRepFlowRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.qrepConfig = QRepConfig.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.createCatalogEntry = reader.bool();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateQRepFlowRequest {
    return {
      qrepConfig: isSet(object.qrepConfig) ? QRepConfig.fromJSON(object.qrepConfig) : undefined,
      createCatalogEntry: isSet(object.createCatalogEntry) ? Boolean(object.createCatalogEntry) : false,
    };
  },

  toJSON(message: CreateQRepFlowRequest): unknown {
    const obj: any = {};
    if (message.qrepConfig !== undefined) {
      obj.qrepConfig = QRepConfig.toJSON(message.qrepConfig);
    }
    if (message.createCatalogEntry === true) {
      obj.createCatalogEntry = message.createCatalogEntry;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateQRepFlowRequest>, I>>(base?: I): CreateQRepFlowRequest {
    return CreateQRepFlowRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateQRepFlowRequest>, I>>(object: I): CreateQRepFlowRequest {
    const message = createBaseCreateQRepFlowRequest();
    message.qrepConfig = (object.qrepConfig !== undefined && object.qrepConfig !== null)
      ? QRepConfig.fromPartial(object.qrepConfig)
      : undefined;
    message.createCatalogEntry = object.createCatalogEntry ?? false;
    return message;
  },
};

function createBaseCreateQRepFlowResponse(): CreateQRepFlowResponse {
  return { worflowId: "" };
}

export const CreateQRepFlowResponse = {
  encode(message: CreateQRepFlowResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.worflowId !== "") {
      writer.uint32(10).string(message.worflowId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateQRepFlowResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateQRepFlowResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.worflowId = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateQRepFlowResponse {
    return { worflowId: isSet(object.worflowId) ? String(object.worflowId) : "" };
  },

  toJSON(message: CreateQRepFlowResponse): unknown {
    const obj: any = {};
    if (message.worflowId !== "") {
      obj.worflowId = message.worflowId;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreateQRepFlowResponse>, I>>(base?: I): CreateQRepFlowResponse {
    return CreateQRepFlowResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreateQRepFlowResponse>, I>>(object: I): CreateQRepFlowResponse {
    const message = createBaseCreateQRepFlowResponse();
    message.worflowId = object.worflowId ?? "";
    return message;
  },
};

function createBaseShutdownRequest(): ShutdownRequest {
  return { workflowId: "", flowJobName: "", sourcePeer: undefined, destinationPeer: undefined };
}

export const ShutdownRequest = {
  encode(message: ShutdownRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.workflowId !== "") {
      writer.uint32(10).string(message.workflowId);
    }
    if (message.flowJobName !== "") {
      writer.uint32(18).string(message.flowJobName);
    }
    if (message.sourcePeer !== undefined) {
      Peer.encode(message.sourcePeer, writer.uint32(26).fork()).ldelim();
    }
    if (message.destinationPeer !== undefined) {
      Peer.encode(message.destinationPeer, writer.uint32(34).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ShutdownRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseShutdownRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.workflowId = reader.string();
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

          message.sourcePeer = Peer.decode(reader, reader.uint32());
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.destinationPeer = Peer.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ShutdownRequest {
    return {
      workflowId: isSet(object.workflowId) ? String(object.workflowId) : "",
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      sourcePeer: isSet(object.sourcePeer) ? Peer.fromJSON(object.sourcePeer) : undefined,
      destinationPeer: isSet(object.destinationPeer) ? Peer.fromJSON(object.destinationPeer) : undefined,
    };
  },

  toJSON(message: ShutdownRequest): unknown {
    const obj: any = {};
    if (message.workflowId !== "") {
      obj.workflowId = message.workflowId;
    }
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.sourcePeer !== undefined) {
      obj.sourcePeer = Peer.toJSON(message.sourcePeer);
    }
    if (message.destinationPeer !== undefined) {
      obj.destinationPeer = Peer.toJSON(message.destinationPeer);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ShutdownRequest>, I>>(base?: I): ShutdownRequest {
    return ShutdownRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ShutdownRequest>, I>>(object: I): ShutdownRequest {
    const message = createBaseShutdownRequest();
    message.workflowId = object.workflowId ?? "";
    message.flowJobName = object.flowJobName ?? "";
    message.sourcePeer = (object.sourcePeer !== undefined && object.sourcePeer !== null)
      ? Peer.fromPartial(object.sourcePeer)
      : undefined;
    message.destinationPeer = (object.destinationPeer !== undefined && object.destinationPeer !== null)
      ? Peer.fromPartial(object.destinationPeer)
      : undefined;
    return message;
  },
};

function createBaseShutdownResponse(): ShutdownResponse {
  return { ok: false, errorMessage: "" };
}

export const ShutdownResponse = {
  encode(message: ShutdownResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.ok === true) {
      writer.uint32(8).bool(message.ok);
    }
    if (message.errorMessage !== "") {
      writer.uint32(18).string(message.errorMessage);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ShutdownResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseShutdownResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.ok = reader.bool();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.errorMessage = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ShutdownResponse {
    return {
      ok: isSet(object.ok) ? Boolean(object.ok) : false,
      errorMessage: isSet(object.errorMessage) ? String(object.errorMessage) : "",
    };
  },

  toJSON(message: ShutdownResponse): unknown {
    const obj: any = {};
    if (message.ok === true) {
      obj.ok = message.ok;
    }
    if (message.errorMessage !== "") {
      obj.errorMessage = message.errorMessage;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ShutdownResponse>, I>>(base?: I): ShutdownResponse {
    return ShutdownResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ShutdownResponse>, I>>(object: I): ShutdownResponse {
    const message = createBaseShutdownResponse();
    message.ok = object.ok ?? false;
    message.errorMessage = object.errorMessage ?? "";
    return message;
  },
};

function createBaseValidatePeerRequest(): ValidatePeerRequest {
  return { peer: undefined };
}

export const ValidatePeerRequest = {
  encode(message: ValidatePeerRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peer !== undefined) {
      Peer.encode(message.peer, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ValidatePeerRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseValidatePeerRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peer = Peer.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ValidatePeerRequest {
    return { peer: isSet(object.peer) ? Peer.fromJSON(object.peer) : undefined };
  },

  toJSON(message: ValidatePeerRequest): unknown {
    const obj: any = {};
    if (message.peer !== undefined) {
      obj.peer = Peer.toJSON(message.peer);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ValidatePeerRequest>, I>>(base?: I): ValidatePeerRequest {
    return ValidatePeerRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ValidatePeerRequest>, I>>(object: I): ValidatePeerRequest {
    const message = createBaseValidatePeerRequest();
    message.peer = (object.peer !== undefined && object.peer !== null) ? Peer.fromPartial(object.peer) : undefined;
    return message;
  },
};

function createBaseCreatePeerRequest(): CreatePeerRequest {
  return { peer: undefined };
}

export const CreatePeerRequest = {
  encode(message: CreatePeerRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peer !== undefined) {
      Peer.encode(message.peer, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreatePeerRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreatePeerRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peer = Peer.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreatePeerRequest {
    return { peer: isSet(object.peer) ? Peer.fromJSON(object.peer) : undefined };
  },

  toJSON(message: CreatePeerRequest): unknown {
    const obj: any = {};
    if (message.peer !== undefined) {
      obj.peer = Peer.toJSON(message.peer);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreatePeerRequest>, I>>(base?: I): CreatePeerRequest {
    return CreatePeerRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreatePeerRequest>, I>>(object: I): CreatePeerRequest {
    const message = createBaseCreatePeerRequest();
    message.peer = (object.peer !== undefined && object.peer !== null) ? Peer.fromPartial(object.peer) : undefined;
    return message;
  },
};

function createBaseValidatePeerResponse(): ValidatePeerResponse {
  return { status: 0, message: "" };
}

export const ValidatePeerResponse = {
  encode(message: ValidatePeerResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== 0) {
      writer.uint32(8).int32(message.status);
    }
    if (message.message !== "") {
      writer.uint32(18).string(message.message);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ValidatePeerResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseValidatePeerResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.status = reader.int32() as any;
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.message = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ValidatePeerResponse {
    return {
      status: isSet(object.status) ? validatePeerStatusFromJSON(object.status) : 0,
      message: isSet(object.message) ? String(object.message) : "",
    };
  },

  toJSON(message: ValidatePeerResponse): unknown {
    const obj: any = {};
    if (message.status !== 0) {
      obj.status = validatePeerStatusToJSON(message.status);
    }
    if (message.message !== "") {
      obj.message = message.message;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ValidatePeerResponse>, I>>(base?: I): ValidatePeerResponse {
    return ValidatePeerResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ValidatePeerResponse>, I>>(object: I): ValidatePeerResponse {
    const message = createBaseValidatePeerResponse();
    message.status = object.status ?? 0;
    message.message = object.message ?? "";
    return message;
  },
};

function createBaseCreatePeerResponse(): CreatePeerResponse {
  return { status: 0, message: "" };
}

export const CreatePeerResponse = {
  encode(message: CreatePeerResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== 0) {
      writer.uint32(8).int32(message.status);
    }
    if (message.message !== "") {
      writer.uint32(18).string(message.message);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreatePeerResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreatePeerResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.status = reader.int32() as any;
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.message = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreatePeerResponse {
    return {
      status: isSet(object.status) ? createPeerStatusFromJSON(object.status) : 0,
      message: isSet(object.message) ? String(object.message) : "",
    };
  },

  toJSON(message: CreatePeerResponse): unknown {
    const obj: any = {};
    if (message.status !== 0) {
      obj.status = createPeerStatusToJSON(message.status);
    }
    if (message.message !== "") {
      obj.message = message.message;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreatePeerResponse>, I>>(base?: I): CreatePeerResponse {
    return CreatePeerResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreatePeerResponse>, I>>(object: I): CreatePeerResponse {
    const message = createBaseCreatePeerResponse();
    message.status = object.status ?? 0;
    message.message = object.message ?? "";
    return message;
  },
};

function createBaseMirrorStatusRequest(): MirrorStatusRequest {
  return { flowJobName: "" };
}

export const MirrorStatusRequest = {
  encode(message: MirrorStatusRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(10).string(message.flowJobName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MirrorStatusRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMirrorStatusRequest();
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

  fromJSON(object: any): MirrorStatusRequest {
    return { flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "" };
  },

  toJSON(message: MirrorStatusRequest): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<MirrorStatusRequest>, I>>(base?: I): MirrorStatusRequest {
    return MirrorStatusRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<MirrorStatusRequest>, I>>(object: I): MirrorStatusRequest {
    const message = createBaseMirrorStatusRequest();
    message.flowJobName = object.flowJobName ?? "";
    return message;
  },
};

function createBasePartitionStatus(): PartitionStatus {
  return { partitionId: "", startTime: undefined, endTime: undefined, numRows: 0 };
}

export const PartitionStatus = {
  encode(message: PartitionStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.partitionId !== "") {
      writer.uint32(10).string(message.partitionId);
    }
    if (message.startTime !== undefined) {
      Timestamp.encode(toTimestamp(message.startTime), writer.uint32(18).fork()).ldelim();
    }
    if (message.endTime !== undefined) {
      Timestamp.encode(toTimestamp(message.endTime), writer.uint32(26).fork()).ldelim();
    }
    if (message.numRows !== 0) {
      writer.uint32(32).int32(message.numRows);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PartitionStatus {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePartitionStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.partitionId = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.startTime = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.endTime = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
        case 4:
          if (tag !== 32) {
            break;
          }

          message.numRows = reader.int32();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PartitionStatus {
    return {
      partitionId: isSet(object.partitionId) ? String(object.partitionId) : "",
      startTime: isSet(object.startTime) ? fromJsonTimestamp(object.startTime) : undefined,
      endTime: isSet(object.endTime) ? fromJsonTimestamp(object.endTime) : undefined,
      numRows: isSet(object.numRows) ? Number(object.numRows) : 0,
    };
  },

  toJSON(message: PartitionStatus): unknown {
    const obj: any = {};
    if (message.partitionId !== "") {
      obj.partitionId = message.partitionId;
    }
    if (message.startTime !== undefined) {
      obj.startTime = message.startTime.toISOString();
    }
    if (message.endTime !== undefined) {
      obj.endTime = message.endTime.toISOString();
    }
    if (message.numRows !== 0) {
      obj.numRows = Math.round(message.numRows);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PartitionStatus>, I>>(base?: I): PartitionStatus {
    return PartitionStatus.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PartitionStatus>, I>>(object: I): PartitionStatus {
    const message = createBasePartitionStatus();
    message.partitionId = object.partitionId ?? "";
    message.startTime = object.startTime ?? undefined;
    message.endTime = object.endTime ?? undefined;
    message.numRows = object.numRows ?? 0;
    return message;
  },
};

function createBaseQRepMirrorStatus(): QRepMirrorStatus {
  return { config: undefined, partitions: [] };
}

export const QRepMirrorStatus = {
  encode(message: QRepMirrorStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.config !== undefined) {
      QRepConfig.encode(message.config, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.partitions) {
      PartitionStatus.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): QRepMirrorStatus {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseQRepMirrorStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.config = QRepConfig.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.partitions.push(PartitionStatus.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): QRepMirrorStatus {
    return {
      config: isSet(object.config) ? QRepConfig.fromJSON(object.config) : undefined,
      partitions: Array.isArray(object?.partitions)
        ? object.partitions.map((e: any) => PartitionStatus.fromJSON(e))
        : [],
    };
  },

  toJSON(message: QRepMirrorStatus): unknown {
    const obj: any = {};
    if (message.config !== undefined) {
      obj.config = QRepConfig.toJSON(message.config);
    }
    if (message.partitions?.length) {
      obj.partitions = message.partitions.map((e) => PartitionStatus.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<QRepMirrorStatus>, I>>(base?: I): QRepMirrorStatus {
    return QRepMirrorStatus.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<QRepMirrorStatus>, I>>(object: I): QRepMirrorStatus {
    const message = createBaseQRepMirrorStatus();
    message.config = (object.config !== undefined && object.config !== null)
      ? QRepConfig.fromPartial(object.config)
      : undefined;
    message.partitions = object.partitions?.map((e) => PartitionStatus.fromPartial(e)) || [];
    return message;
  },
};

function createBaseCDCSyncStatus(): CDCSyncStatus {
  return { startLsn: 0, endLsn: 0, numRows: 0, startTime: undefined, endTime: undefined };
}

export const CDCSyncStatus = {
  encode(message: CDCSyncStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.startLsn !== 0) {
      writer.uint32(8).int64(message.startLsn);
    }
    if (message.endLsn !== 0) {
      writer.uint32(16).int64(message.endLsn);
    }
    if (message.numRows !== 0) {
      writer.uint32(24).int32(message.numRows);
    }
    if (message.startTime !== undefined) {
      Timestamp.encode(toTimestamp(message.startTime), writer.uint32(34).fork()).ldelim();
    }
    if (message.endTime !== undefined) {
      Timestamp.encode(toTimestamp(message.endTime), writer.uint32(42).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CDCSyncStatus {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCDCSyncStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.startLsn = longToNumber(reader.int64() as Long);
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.endLsn = longToNumber(reader.int64() as Long);
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.numRows = reader.int32();
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.startTime = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.endTime = fromTimestamp(Timestamp.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CDCSyncStatus {
    return {
      startLsn: isSet(object.startLsn) ? Number(object.startLsn) : 0,
      endLsn: isSet(object.endLsn) ? Number(object.endLsn) : 0,
      numRows: isSet(object.numRows) ? Number(object.numRows) : 0,
      startTime: isSet(object.startTime) ? fromJsonTimestamp(object.startTime) : undefined,
      endTime: isSet(object.endTime) ? fromJsonTimestamp(object.endTime) : undefined,
    };
  },

  toJSON(message: CDCSyncStatus): unknown {
    const obj: any = {};
    if (message.startLsn !== 0) {
      obj.startLsn = Math.round(message.startLsn);
    }
    if (message.endLsn !== 0) {
      obj.endLsn = Math.round(message.endLsn);
    }
    if (message.numRows !== 0) {
      obj.numRows = Math.round(message.numRows);
    }
    if (message.startTime !== undefined) {
      obj.startTime = message.startTime.toISOString();
    }
    if (message.endTime !== undefined) {
      obj.endTime = message.endTime.toISOString();
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CDCSyncStatus>, I>>(base?: I): CDCSyncStatus {
    return CDCSyncStatus.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CDCSyncStatus>, I>>(object: I): CDCSyncStatus {
    const message = createBaseCDCSyncStatus();
    message.startLsn = object.startLsn ?? 0;
    message.endLsn = object.endLsn ?? 0;
    message.numRows = object.numRows ?? 0;
    message.startTime = object.startTime ?? undefined;
    message.endTime = object.endTime ?? undefined;
    return message;
  },
};

function createBasePeerDataRequest(): PeerDataRequest {
  return { peerName: "" };
}

export const PeerDataRequest = {
  encode(message: PeerDataRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.peerName !== "") {
      writer.uint32(10).string(message.peerName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PeerDataRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePeerDataRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peerName = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PeerDataRequest {
    return { peerName: isSet(object.peerName) ? String(object.peerName) : "" };
  },

  toJSON(message: PeerDataRequest): unknown {
    const obj: any = {};
    if (message.peerName !== "") {
      obj.peerName = message.peerName;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PeerDataRequest>, I>>(base?: I): PeerDataRequest {
    return PeerDataRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PeerDataRequest>, I>>(object: I): PeerDataRequest {
    const message = createBasePeerDataRequest();
    message.peerName = object.peerName ?? "";
    return message;
  },
};

function createBaseSlotInfo(): SlotInfo {
  return { slotName: "", redoLSN: "", restartLSN: "", lagInMb: 0 };
}

export const SlotInfo = {
  encode(message: SlotInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.slotName !== "") {
      writer.uint32(10).string(message.slotName);
    }
    if (message.redoLSN !== "") {
      writer.uint32(18).string(message.redoLSN);
    }
    if (message.restartLSN !== "") {
      writer.uint32(26).string(message.restartLSN);
    }
    if (message.lagInMb !== 0) {
      writer.uint32(37).float(message.lagInMb);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SlotInfo {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSlotInfo();
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

          message.redoLSN = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.restartLSN = reader.string();
          continue;
        case 4:
          if (tag !== 37) {
            break;
          }

          message.lagInMb = reader.float();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SlotInfo {
    return {
      slotName: isSet(object.slotName) ? String(object.slotName) : "",
      redoLSN: isSet(object.redoLSN) ? String(object.redoLSN) : "",
      restartLSN: isSet(object.restartLSN) ? String(object.restartLSN) : "",
      lagInMb: isSet(object.lagInMb) ? Number(object.lagInMb) : 0,
    };
  },

  toJSON(message: SlotInfo): unknown {
    const obj: any = {};
    if (message.slotName !== "") {
      obj.slotName = message.slotName;
    }
    if (message.redoLSN !== "") {
      obj.redoLSN = message.redoLSN;
    }
    if (message.restartLSN !== "") {
      obj.restartLSN = message.restartLSN;
    }
    if (message.lagInMb !== 0) {
      obj.lagInMb = message.lagInMb;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SlotInfo>, I>>(base?: I): SlotInfo {
    return SlotInfo.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SlotInfo>, I>>(object: I): SlotInfo {
    const message = createBaseSlotInfo();
    message.slotName = object.slotName ?? "";
    message.redoLSN = object.redoLSN ?? "";
    message.restartLSN = object.restartLSN ?? "";
    message.lagInMb = object.lagInMb ?? 0;
    return message;
  },
};

function createBaseStatInfo(): StatInfo {
  return { pid: 0, query: "", duration: 0 };
}

export const StatInfo = {
  encode(message: StatInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.pid !== 0) {
      writer.uint32(8).int64(message.pid);
    }
    if (message.query !== "") {
      writer.uint32(18).string(message.query);
    }
    if (message.duration !== 0) {
      writer.uint32(29).float(message.duration);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StatInfo {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStatInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.pid = longToNumber(reader.int64() as Long);
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.query = reader.string();
          continue;
        case 3:
          if (tag !== 29) {
            break;
          }

          message.duration = reader.float();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): StatInfo {
    return {
      pid: isSet(object.pid) ? Number(object.pid) : 0,
      query: isSet(object.query) ? String(object.query) : "",
      duration: isSet(object.duration) ? Number(object.duration) : 0,
    };
  },

  toJSON(message: StatInfo): unknown {
    const obj: any = {};
    if (message.pid !== 0) {
      obj.pid = Math.round(message.pid);
    }
    if (message.query !== "") {
      obj.query = message.query;
    }
    if (message.duration !== 0) {
      obj.duration = message.duration;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<StatInfo>, I>>(base?: I): StatInfo {
    return StatInfo.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<StatInfo>, I>>(object: I): StatInfo {
    const message = createBaseStatInfo();
    message.pid = object.pid ?? 0;
    message.query = object.query ?? "";
    message.duration = object.duration ?? 0;
    return message;
  },
};

function createBasePeerSlotResponse(): PeerSlotResponse {
  return { slotData: [] };
}

export const PeerSlotResponse = {
  encode(message: PeerSlotResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.slotData) {
      SlotInfo.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PeerSlotResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePeerSlotResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.slotData.push(SlotInfo.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PeerSlotResponse {
    return { slotData: Array.isArray(object?.slotData) ? object.slotData.map((e: any) => SlotInfo.fromJSON(e)) : [] };
  },

  toJSON(message: PeerSlotResponse): unknown {
    const obj: any = {};
    if (message.slotData?.length) {
      obj.slotData = message.slotData.map((e) => SlotInfo.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PeerSlotResponse>, I>>(base?: I): PeerSlotResponse {
    return PeerSlotResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PeerSlotResponse>, I>>(object: I): PeerSlotResponse {
    const message = createBasePeerSlotResponse();
    message.slotData = object.slotData?.map((e) => SlotInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBasePeerStatResponse(): PeerStatResponse {
  return { statData: [] };
}

export const PeerStatResponse = {
  encode(message: PeerStatResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.statData) {
      StatInfo.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PeerStatResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePeerStatResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.statData.push(StatInfo.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PeerStatResponse {
    return { statData: Array.isArray(object?.statData) ? object.statData.map((e: any) => StatInfo.fromJSON(e)) : [] };
  },

  toJSON(message: PeerStatResponse): unknown {
    const obj: any = {};
    if (message.statData?.length) {
      obj.statData = message.statData.map((e) => StatInfo.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PeerStatResponse>, I>>(base?: I): PeerStatResponse {
    return PeerStatResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PeerStatResponse>, I>>(object: I): PeerStatResponse {
    const message = createBasePeerStatResponse();
    message.statData = object.statData?.map((e) => StatInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSnapshotStatus(): SnapshotStatus {
  return { clones: [] };
}

export const SnapshotStatus = {
  encode(message: SnapshotStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.clones) {
      QRepMirrorStatus.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SnapshotStatus {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSnapshotStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.clones.push(QRepMirrorStatus.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SnapshotStatus {
    return { clones: Array.isArray(object?.clones) ? object.clones.map((e: any) => QRepMirrorStatus.fromJSON(e)) : [] };
  },

  toJSON(message: SnapshotStatus): unknown {
    const obj: any = {};
    if (message.clones?.length) {
      obj.clones = message.clones.map((e) => QRepMirrorStatus.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SnapshotStatus>, I>>(base?: I): SnapshotStatus {
    return SnapshotStatus.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SnapshotStatus>, I>>(object: I): SnapshotStatus {
    const message = createBaseSnapshotStatus();
    message.clones = object.clones?.map((e) => QRepMirrorStatus.fromPartial(e)) || [];
    return message;
  },
};

function createBaseCDCMirrorStatus(): CDCMirrorStatus {
  return { config: undefined, snapshotStatus: undefined, cdcSyncs: [] };
}

export const CDCMirrorStatus = {
  encode(message: CDCMirrorStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.config !== undefined) {
      FlowConnectionConfigs.encode(message.config, writer.uint32(10).fork()).ldelim();
    }
    if (message.snapshotStatus !== undefined) {
      SnapshotStatus.encode(message.snapshotStatus, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.cdcSyncs) {
      CDCSyncStatus.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CDCMirrorStatus {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCDCMirrorStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.config = FlowConnectionConfigs.decode(reader, reader.uint32());
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.snapshotStatus = SnapshotStatus.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.cdcSyncs.push(CDCSyncStatus.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CDCMirrorStatus {
    return {
      config: isSet(object.config) ? FlowConnectionConfigs.fromJSON(object.config) : undefined,
      snapshotStatus: isSet(object.snapshotStatus) ? SnapshotStatus.fromJSON(object.snapshotStatus) : undefined,
      cdcSyncs: Array.isArray(object?.cdcSyncs) ? object.cdcSyncs.map((e: any) => CDCSyncStatus.fromJSON(e)) : [],
    };
  },

  toJSON(message: CDCMirrorStatus): unknown {
    const obj: any = {};
    if (message.config !== undefined) {
      obj.config = FlowConnectionConfigs.toJSON(message.config);
    }
    if (message.snapshotStatus !== undefined) {
      obj.snapshotStatus = SnapshotStatus.toJSON(message.snapshotStatus);
    }
    if (message.cdcSyncs?.length) {
      obj.cdcSyncs = message.cdcSyncs.map((e) => CDCSyncStatus.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CDCMirrorStatus>, I>>(base?: I): CDCMirrorStatus {
    return CDCMirrorStatus.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CDCMirrorStatus>, I>>(object: I): CDCMirrorStatus {
    const message = createBaseCDCMirrorStatus();
    message.config = (object.config !== undefined && object.config !== null)
      ? FlowConnectionConfigs.fromPartial(object.config)
      : undefined;
    message.snapshotStatus = (object.snapshotStatus !== undefined && object.snapshotStatus !== null)
      ? SnapshotStatus.fromPartial(object.snapshotStatus)
      : undefined;
    message.cdcSyncs = object.cdcSyncs?.map((e) => CDCSyncStatus.fromPartial(e)) || [];
    return message;
  },
};

function createBaseMirrorStatusResponse(): MirrorStatusResponse {
  return { flowJobName: "", qrepStatus: undefined, cdcStatus: undefined, errorMessage: "" };
}

export const MirrorStatusResponse = {
  encode(message: MirrorStatusResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.flowJobName !== "") {
      writer.uint32(10).string(message.flowJobName);
    }
    if (message.qrepStatus !== undefined) {
      QRepMirrorStatus.encode(message.qrepStatus, writer.uint32(18).fork()).ldelim();
    }
    if (message.cdcStatus !== undefined) {
      CDCMirrorStatus.encode(message.cdcStatus, writer.uint32(26).fork()).ldelim();
    }
    if (message.errorMessage !== "") {
      writer.uint32(34).string(message.errorMessage);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MirrorStatusResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMirrorStatusResponse();
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

          message.qrepStatus = QRepMirrorStatus.decode(reader, reader.uint32());
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.cdcStatus = CDCMirrorStatus.decode(reader, reader.uint32());
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.errorMessage = reader.string();
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): MirrorStatusResponse {
    return {
      flowJobName: isSet(object.flowJobName) ? String(object.flowJobName) : "",
      qrepStatus: isSet(object.qrepStatus) ? QRepMirrorStatus.fromJSON(object.qrepStatus) : undefined,
      cdcStatus: isSet(object.cdcStatus) ? CDCMirrorStatus.fromJSON(object.cdcStatus) : undefined,
      errorMessage: isSet(object.errorMessage) ? String(object.errorMessage) : "",
    };
  },

  toJSON(message: MirrorStatusResponse): unknown {
    const obj: any = {};
    if (message.flowJobName !== "") {
      obj.flowJobName = message.flowJobName;
    }
    if (message.qrepStatus !== undefined) {
      obj.qrepStatus = QRepMirrorStatus.toJSON(message.qrepStatus);
    }
    if (message.cdcStatus !== undefined) {
      obj.cdcStatus = CDCMirrorStatus.toJSON(message.cdcStatus);
    }
    if (message.errorMessage !== "") {
      obj.errorMessage = message.errorMessage;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<MirrorStatusResponse>, I>>(base?: I): MirrorStatusResponse {
    return MirrorStatusResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<MirrorStatusResponse>, I>>(object: I): MirrorStatusResponse {
    const message = createBaseMirrorStatusResponse();
    message.flowJobName = object.flowJobName ?? "";
    message.qrepStatus = (object.qrepStatus !== undefined && object.qrepStatus !== null)
      ? QRepMirrorStatus.fromPartial(object.qrepStatus)
      : undefined;
    message.cdcStatus = (object.cdcStatus !== undefined && object.cdcStatus !== null)
      ? CDCMirrorStatus.fromPartial(object.cdcStatus)
      : undefined;
    message.errorMessage = object.errorMessage ?? "";
    return message;
  },
};

export type FlowServiceService = typeof FlowServiceService;
export const FlowServiceService = {
  validatePeer: {
    path: "/peerdb_route.FlowService/ValidatePeer",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: ValidatePeerRequest) => Buffer.from(ValidatePeerRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => ValidatePeerRequest.decode(value),
    responseSerialize: (value: ValidatePeerResponse) => Buffer.from(ValidatePeerResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => ValidatePeerResponse.decode(value),
  },
  createPeer: {
    path: "/peerdb_route.FlowService/CreatePeer",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: CreatePeerRequest) => Buffer.from(CreatePeerRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => CreatePeerRequest.decode(value),
    responseSerialize: (value: CreatePeerResponse) => Buffer.from(CreatePeerResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => CreatePeerResponse.decode(value),
  },
  createCdcFlow: {
    path: "/peerdb_route.FlowService/CreateCDCFlow",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: CreateCDCFlowRequest) => Buffer.from(CreateCDCFlowRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => CreateCDCFlowRequest.decode(value),
    responseSerialize: (value: CreateCDCFlowResponse) => Buffer.from(CreateCDCFlowResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => CreateCDCFlowResponse.decode(value),
  },
  createQRepFlow: {
    path: "/peerdb_route.FlowService/CreateQRepFlow",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: CreateQRepFlowRequest) => Buffer.from(CreateQRepFlowRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => CreateQRepFlowRequest.decode(value),
    responseSerialize: (value: CreateQRepFlowResponse) => Buffer.from(CreateQRepFlowResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => CreateQRepFlowResponse.decode(value),
  },
  getSlotInfo: {
    path: "/peerdb_route.FlowService/GetSlotInfo",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: PeerDataRequest) => Buffer.from(PeerDataRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => PeerDataRequest.decode(value),
    responseSerialize: (value: PeerSlotResponse) => Buffer.from(PeerSlotResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => PeerSlotResponse.decode(value),
  },
  getStatInfo: {
    path: "/peerdb_route.FlowService/GetStatInfo",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: PeerDataRequest) => Buffer.from(PeerDataRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => PeerDataRequest.decode(value),
    responseSerialize: (value: PeerStatResponse) => Buffer.from(PeerStatResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => PeerStatResponse.decode(value),
  },
  shutdownFlow: {
    path: "/peerdb_route.FlowService/ShutdownFlow",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: ShutdownRequest) => Buffer.from(ShutdownRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => ShutdownRequest.decode(value),
    responseSerialize: (value: ShutdownResponse) => Buffer.from(ShutdownResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => ShutdownResponse.decode(value),
  },
  mirrorStatus: {
    path: "/peerdb_route.FlowService/MirrorStatus",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: MirrorStatusRequest) => Buffer.from(MirrorStatusRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => MirrorStatusRequest.decode(value),
    responseSerialize: (value: MirrorStatusResponse) => Buffer.from(MirrorStatusResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => MirrorStatusResponse.decode(value),
  },
} as const;

export interface FlowServiceServer extends UntypedServiceImplementation {
  validatePeer: handleUnaryCall<ValidatePeerRequest, ValidatePeerResponse>;
  createPeer: handleUnaryCall<CreatePeerRequest, CreatePeerResponse>;
  createCdcFlow: handleUnaryCall<CreateCDCFlowRequest, CreateCDCFlowResponse>;
  createQRepFlow: handleUnaryCall<CreateQRepFlowRequest, CreateQRepFlowResponse>;
  getSlotInfo: handleUnaryCall<PeerDataRequest, PeerSlotResponse>;
  getStatInfo: handleUnaryCall<PeerDataRequest, PeerStatResponse>;
  shutdownFlow: handleUnaryCall<ShutdownRequest, ShutdownResponse>;
  mirrorStatus: handleUnaryCall<MirrorStatusRequest, MirrorStatusResponse>;
}

export interface FlowServiceClient extends Client {
  validatePeer(
    request: ValidatePeerRequest,
    callback: (error: ServiceError | null, response: ValidatePeerResponse) => void,
  ): ClientUnaryCall;
  validatePeer(
    request: ValidatePeerRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: ValidatePeerResponse) => void,
  ): ClientUnaryCall;
  validatePeer(
    request: ValidatePeerRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: ValidatePeerResponse) => void,
  ): ClientUnaryCall;
  createPeer(
    request: CreatePeerRequest,
    callback: (error: ServiceError | null, response: CreatePeerResponse) => void,
  ): ClientUnaryCall;
  createPeer(
    request: CreatePeerRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: CreatePeerResponse) => void,
  ): ClientUnaryCall;
  createPeer(
    request: CreatePeerRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: CreatePeerResponse) => void,
  ): ClientUnaryCall;
  createCdcFlow(
    request: CreateCDCFlowRequest,
    callback: (error: ServiceError | null, response: CreateCDCFlowResponse) => void,
  ): ClientUnaryCall;
  createCdcFlow(
    request: CreateCDCFlowRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: CreateCDCFlowResponse) => void,
  ): ClientUnaryCall;
  createCdcFlow(
    request: CreateCDCFlowRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: CreateCDCFlowResponse) => void,
  ): ClientUnaryCall;
  createQRepFlow(
    request: CreateQRepFlowRequest,
    callback: (error: ServiceError | null, response: CreateQRepFlowResponse) => void,
  ): ClientUnaryCall;
  createQRepFlow(
    request: CreateQRepFlowRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: CreateQRepFlowResponse) => void,
  ): ClientUnaryCall;
  createQRepFlow(
    request: CreateQRepFlowRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: CreateQRepFlowResponse) => void,
  ): ClientUnaryCall;
  getSlotInfo(
    request: PeerDataRequest,
    callback: (error: ServiceError | null, response: PeerSlotResponse) => void,
  ): ClientUnaryCall;
  getSlotInfo(
    request: PeerDataRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: PeerSlotResponse) => void,
  ): ClientUnaryCall;
  getSlotInfo(
    request: PeerDataRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: PeerSlotResponse) => void,
  ): ClientUnaryCall;
  getStatInfo(
    request: PeerDataRequest,
    callback: (error: ServiceError | null, response: PeerStatResponse) => void,
  ): ClientUnaryCall;
  getStatInfo(
    request: PeerDataRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: PeerStatResponse) => void,
  ): ClientUnaryCall;
  getStatInfo(
    request: PeerDataRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: PeerStatResponse) => void,
  ): ClientUnaryCall;
  shutdownFlow(
    request: ShutdownRequest,
    callback: (error: ServiceError | null, response: ShutdownResponse) => void,
  ): ClientUnaryCall;
  shutdownFlow(
    request: ShutdownRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: ShutdownResponse) => void,
  ): ClientUnaryCall;
  shutdownFlow(
    request: ShutdownRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: ShutdownResponse) => void,
  ): ClientUnaryCall;
  mirrorStatus(
    request: MirrorStatusRequest,
    callback: (error: ServiceError | null, response: MirrorStatusResponse) => void,
  ): ClientUnaryCall;
  mirrorStatus(
    request: MirrorStatusRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: MirrorStatusResponse) => void,
  ): ClientUnaryCall;
  mirrorStatus(
    request: MirrorStatusRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: MirrorStatusResponse) => void,
  ): ClientUnaryCall;
}

export const FlowServiceClient = makeGenericClientConstructor(
  FlowServiceService,
  "peerdb_route.FlowService",
) as unknown as {
  new (address: string, credentials: ChannelCredentials, options?: Partial<ClientOptions>): FlowServiceClient;
  service: typeof FlowServiceService;
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
