/* eslint-disable */
import _m0 from "protobufjs/minimal";
import { FlowConnectionConfigs, QRepConfig } from "./flow";
import { Peer } from "./peers";

export const protobufPackage = "peerdb_route";

export interface CreatePeerFlowRequest {
  connectionConfigs: FlowConnectionConfigs | undefined;
}

export interface CreatePeerFlowResponse {
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

function createBaseCreatePeerFlowRequest(): CreatePeerFlowRequest {
  return { connectionConfigs: undefined };
}

export const CreatePeerFlowRequest = {
  encode(message: CreatePeerFlowRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.connectionConfigs !== undefined) {
      FlowConnectionConfigs.encode(message.connectionConfigs, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreatePeerFlowRequest {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreatePeerFlowRequest();
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

  fromJSON(object: any): CreatePeerFlowRequest {
    return {
      connectionConfigs: isSet(object.connectionConfigs)
        ? FlowConnectionConfigs.fromJSON(object.connectionConfigs)
        : undefined,
    };
  },

  toJSON(message: CreatePeerFlowRequest): unknown {
    const obj: any = {};
    if (message.connectionConfigs !== undefined) {
      obj.connectionConfigs = FlowConnectionConfigs.toJSON(message.connectionConfigs);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreatePeerFlowRequest>, I>>(base?: I): CreatePeerFlowRequest {
    return CreatePeerFlowRequest.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreatePeerFlowRequest>, I>>(object: I): CreatePeerFlowRequest {
    const message = createBaseCreatePeerFlowRequest();
    message.connectionConfigs = (object.connectionConfigs !== undefined && object.connectionConfigs !== null)
      ? FlowConnectionConfigs.fromPartial(object.connectionConfigs)
      : undefined;
    return message;
  },
};

function createBaseCreatePeerFlowResponse(): CreatePeerFlowResponse {
  return { worflowId: "" };
}

export const CreatePeerFlowResponse = {
  encode(message: CreatePeerFlowResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.worflowId !== "") {
      writer.uint32(10).string(message.worflowId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreatePeerFlowResponse {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreatePeerFlowResponse();
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

  fromJSON(object: any): CreatePeerFlowResponse {
    return { worflowId: isSet(object.worflowId) ? String(object.worflowId) : "" };
  },

  toJSON(message: CreatePeerFlowResponse): unknown {
    const obj: any = {};
    if (message.worflowId !== "") {
      obj.worflowId = message.worflowId;
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<CreatePeerFlowResponse>, I>>(base?: I): CreatePeerFlowResponse {
    return CreatePeerFlowResponse.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<CreatePeerFlowResponse>, I>>(object: I): CreatePeerFlowResponse {
    const message = createBaseCreatePeerFlowResponse();
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

export interface FlowService {
  ListPeers(request: ListPeersRequest): Promise<ListPeersResponse>;
  CreatePeerFlow(request: CreatePeerFlowRequest): Promise<CreatePeerFlowResponse>;
  CreateQRepFlow(request: CreateQRepFlowRequest): Promise<CreateQRepFlowResponse>;
  ShutdownFlow(request: ShutdownRequest): Promise<ShutdownResponse>;
}

export const FlowServiceServiceName = "peerdb_route.FlowService";
export class FlowServiceClientImpl implements FlowService {
  private readonly rpc: Rpc;
  private readonly service: string;
  constructor(rpc: Rpc, opts?: { service?: string }) {
    this.service = opts?.service || FlowServiceServiceName;
    this.rpc = rpc;
    this.ListPeers = this.ListPeers.bind(this);
    this.CreatePeerFlow = this.CreatePeerFlow.bind(this);
    this.CreateQRepFlow = this.CreateQRepFlow.bind(this);
    this.ShutdownFlow = this.ShutdownFlow.bind(this);
  }
  ListPeers(request: ListPeersRequest): Promise<ListPeersResponse> {
    const data = ListPeersRequest.encode(request).finish();
    const promise = this.rpc.request(this.service, "ListPeers", data);
    return promise.then((data) => ListPeersResponse.decode(_m0.Reader.create(data)));
  }

  CreatePeerFlow(request: CreatePeerFlowRequest): Promise<CreatePeerFlowResponse> {
    const data = CreatePeerFlowRequest.encode(request).finish();
    const promise = this.rpc.request(this.service, "CreatePeerFlow", data);
    return promise.then((data) => CreatePeerFlowResponse.decode(_m0.Reader.create(data)));
  }

  CreateQRepFlow(request: CreateQRepFlowRequest): Promise<CreateQRepFlowResponse> {
    const data = CreateQRepFlowRequest.encode(request).finish();
    const promise = this.rpc.request(this.service, "CreateQRepFlow", data);
    return promise.then((data) => CreateQRepFlowResponse.decode(_m0.Reader.create(data)));
  }

  ShutdownFlow(request: ShutdownRequest): Promise<ShutdownResponse> {
    const data = ShutdownRequest.encode(request).finish();
    const promise = this.rpc.request(this.service, "ShutdownFlow", data);
    return promise.then((data) => ShutdownResponse.decode(_m0.Reader.create(data)));
  }
}

interface Rpc {
  request(service: string, method: string, data: Uint8Array): Promise<Uint8Array>;
}

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
