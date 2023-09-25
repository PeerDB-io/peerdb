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
import _m0 from "protobufjs/minimal";
import { FlowConnectionConfigs, QRepConfig } from "./flow";
import { Peer } from "./peers";

export const protobufPackage = "peerdb_route";

export enum ValidatePeerStatus {
  VALID = 0,
  INVALID = 1,
  VALIDATING = 2,
  ERROR = 3,
  UNRECOGNIZED = -1,
}

export function validatePeerStatusFromJSON(object: any): ValidatePeerStatus {
  switch (object) {
    case 0:
    case "VALID":
      return ValidatePeerStatus.VALID;
    case 1:
    case "INVALID":
      return ValidatePeerStatus.INVALID;
    case 2:
    case "VALIDATING":
      return ValidatePeerStatus.VALIDATING;
    case 3:
    case "ERROR":
      return ValidatePeerStatus.ERROR;
    case -1:
    case "UNRECOGNIZED":
    default:
      return ValidatePeerStatus.UNRECOGNIZED;
  }
}

export function validatePeerStatusToJSON(object: ValidatePeerStatus): string {
  switch (object) {
    case ValidatePeerStatus.VALID:
      return "VALID";
    case ValidatePeerStatus.INVALID:
      return "INVALID";
    case ValidatePeerStatus.VALIDATING:
      return "VALIDATING";
    case ValidatePeerStatus.ERROR:
      return "ERROR";
    case ValidatePeerStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export enum CreatePeerStatus {
  CREATED = 0,
  PENDING = 1,
  FAILED = 2,
  UNRECOGNIZED = -1,
}

export function createPeerStatusFromJSON(object: any): CreatePeerStatus {
  switch (object) {
    case 0:
    case "CREATED":
      return CreatePeerStatus.CREATED;
    case 1:
    case "PENDING":
      return CreatePeerStatus.PENDING;
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
    case CreatePeerStatus.CREATED:
      return "CREATED";
    case CreatePeerStatus.PENDING:
      return "PENDING";
    case CreatePeerStatus.FAILED:
      return "FAILED";
    case CreatePeerStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface CreateCDCFlowRequest {
  connectionConfigs: FlowConnectionConfigs | undefined;
}

export interface CreateCDCFlowResponse {
  worflowId: string;
}

export interface CreateQRepFlowRequest {
  qrepConfig: QRepConfig | undefined;
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

export interface ListPeersRequest {
}

export interface ListPeersResponse {
  peers: Peer[];
}

export interface ValidatePeerRequest {
  name: string;
}

export interface CreatePeerRequest {
  name: string;
}

export interface ValidatePeerResponse {
  status: ValidatePeerStatus;
  message: string;
}

export interface CreatePeerResponse {
  status: CreatePeerStatus;
  message: string;
}

function createBaseCreateCDCFlowRequest(): CreateCDCFlowRequest {
  return { connectionConfigs: undefined };
}

export const CreateCDCFlowRequest = {
  encode(message: CreateCDCFlowRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.connectionConfigs !== undefined) {
      FlowConnectionConfigs.encode(message.connectionConfigs, writer.uint32(10).fork()).ldelim();
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
    };
  },

  toJSON(message: CreateCDCFlowRequest): unknown {
    const obj: any = {};
    if (message.connectionConfigs !== undefined) {
      obj.connectionConfigs = FlowConnectionConfigs.toJSON(message.connectionConfigs);
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
  return { qrepConfig: undefined };
}

export const CreateQRepFlowRequest = {
  encode(message: CreateQRepFlowRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.qrepConfig !== undefined) {
      QRepConfig.encode(message.qrepConfig, writer.uint32(10).fork()).ldelim();
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
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): CreateQRepFlowRequest {
    return { qrepConfig: isSet(object.qrepConfig) ? QRepConfig.fromJSON(object.qrepConfig) : undefined };
  },

  toJSON(message: CreateQRepFlowRequest): unknown {
    const obj: any = {};
    if (message.qrepConfig !== undefined) {
      obj.qrepConfig = QRepConfig.toJSON(message.qrepConfig);
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

function createBaseListPeersRequest(): ListPeersRequest {
  return {};
}

export const ListPeersRequest = {
  encode(_: ListPeersRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListPeersRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListPeersRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(_: any): ListPeersRequest {
    return {};
  },

  toJSON(_: ListPeersRequest): unknown {
    const obj: any = {};
    return obj;
  },

  create<I extends Exact<DeepPartial<ListPeersRequest>, I>>(base?: I): ListPeersRequest {
    return ListPeersRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ListPeersRequest>, I>>(_: I): ListPeersRequest {
    const message = createBaseListPeersRequest();
    return message;
  },
};

function createBaseListPeersResponse(): ListPeersResponse {
  return { peers: [] };
}

export const ListPeersResponse = {
  encode(message: ListPeersResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.peers) {
      Peer.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListPeersResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListPeersResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.peers.push(Peer.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): ListPeersResponse {
    return { peers: Array.isArray(object?.peers) ? object.peers.map((e: any) => Peer.fromJSON(e)) : [] };
  },

  toJSON(message: ListPeersResponse): unknown {
    const obj: any = {};
    if (message.peers?.length) {
      obj.peers = message.peers.map((e) => Peer.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ListPeersResponse>, I>>(base?: I): ListPeersResponse {
    return ListPeersResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ListPeersResponse>, I>>(object: I): ListPeersResponse {
    const message = createBaseListPeersResponse();
    message.peers = object.peers?.map((e) => Peer.fromPartial(e)) || [];
    return message;
  },
};

function createBaseValidatePeerRequest(): ValidatePeerRequest {
  return { name: "" };
}

export const ValidatePeerRequest = {
  encode(message: ValidatePeerRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.name !== "") {
      writer.uint32(10).string(message.name);
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

          message.name = reader.string();
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
    return { name: isSet(object.name) ? String(object.name) : "" };
  },

  toJSON(message: ValidatePeerRequest): unknown {
    const obj: any = {};
    if (message.name !== "") {
      obj.name = message.name;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<ValidatePeerRequest>, I>>(base?: I): ValidatePeerRequest {
    return ValidatePeerRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<ValidatePeerRequest>, I>>(object: I): ValidatePeerRequest {
    const message = createBaseValidatePeerRequest();
    message.name = object.name ?? "";
    return message;
  },
};

function createBaseCreatePeerRequest(): CreatePeerRequest {
  return { name: "" };
}

export const CreatePeerRequest = {
  encode(message: CreatePeerRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.name !== "") {
      writer.uint32(10).string(message.name);
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

          message.name = reader.string();
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
    return { name: isSet(object.name) ? String(object.name) : "" };
  },

  toJSON(message: CreatePeerRequest): unknown {
    const obj: any = {};
    if (message.name !== "") {
      obj.name = message.name;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreatePeerRequest>, I>>(base?: I): CreatePeerRequest {
    return CreatePeerRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreatePeerRequest>, I>>(object: I): CreatePeerRequest {
    const message = createBaseCreatePeerRequest();
    message.name = object.name ?? "";
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

export type FlowServiceService = typeof FlowServiceService;
export const FlowServiceService = {
  listPeers: {
    path: "/peerdb_route.FlowService/ListPeers",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: ListPeersRequest) => Buffer.from(ListPeersRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => ListPeersRequest.decode(value),
    responseSerialize: (value: ListPeersResponse) => Buffer.from(ListPeersResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => ListPeersResponse.decode(value),
  },
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
  shutdownFlow: {
    path: "/peerdb_route.FlowService/ShutdownFlow",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: ShutdownRequest) => Buffer.from(ShutdownRequest.encode(value).finish()),
    requestDeserialize: (value: Buffer) => ShutdownRequest.decode(value),
    responseSerialize: (value: ShutdownResponse) => Buffer.from(ShutdownResponse.encode(value).finish()),
    responseDeserialize: (value: Buffer) => ShutdownResponse.decode(value),
  },
} as const;

export interface FlowServiceServer extends UntypedServiceImplementation {
  listPeers: handleUnaryCall<ListPeersRequest, ListPeersResponse>;
  validatePeer: handleUnaryCall<ValidatePeerRequest, ValidatePeerResponse>;
  createPeer: handleUnaryCall<CreatePeerRequest, CreatePeerResponse>;
  createCdcFlow: handleUnaryCall<CreateCDCFlowRequest, CreateCDCFlowResponse>;
  createQRepFlow: handleUnaryCall<CreateQRepFlowRequest, CreateQRepFlowResponse>;
  shutdownFlow: handleUnaryCall<ShutdownRequest, ShutdownResponse>;
}

export interface FlowServiceClient extends Client {
  listPeers(
    request: ListPeersRequest,
    callback: (error: ServiceError | null, response: ListPeersResponse) => void,
  ): ClientUnaryCall;
  listPeers(
    request: ListPeersRequest,
    metadata: Metadata,
    callback: (error: ServiceError | null, response: ListPeersResponse) => void,
  ): ClientUnaryCall;
  listPeers(
    request: ListPeersRequest,
    metadata: Metadata,
    options: Partial<CallOptions>,
    callback: (error: ServiceError | null, response: ListPeersResponse) => void,
  ): ClientUnaryCall;
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
}

export const FlowServiceClient = makeGenericClientConstructor(
  FlowServiceService,
  "peerdb_route.FlowService",
) as unknown as {
  new (address: string, credentials: ChannelCredentials, options?: Partial<ClientOptions>): FlowServiceClient;
  service: typeof FlowServiceService;
};

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
