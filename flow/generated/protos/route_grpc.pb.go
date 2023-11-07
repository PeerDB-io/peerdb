// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: route.proto

package protos

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	FlowService_ValidatePeer_FullMethodName      = "/peerdb_route.FlowService/ValidatePeer"
	FlowService_CreatePeer_FullMethodName        = "/peerdb_route.FlowService/CreatePeer"
	FlowService_DropPeer_FullMethodName          = "/peerdb_route.FlowService/DropPeer"
	FlowService_CreateCDCFlow_FullMethodName     = "/peerdb_route.FlowService/CreateCDCFlow"
	FlowService_CreateQRepFlow_FullMethodName    = "/peerdb_route.FlowService/CreateQRepFlow"
	FlowService_GetSchemas_FullMethodName        = "/peerdb_route.FlowService/GetSchemas"
	FlowService_GetTablesInSchema_FullMethodName = "/peerdb_route.FlowService/GetTablesInSchema"
	FlowService_GetColumns_FullMethodName        = "/peerdb_route.FlowService/GetColumns"
	FlowService_GetSlotInfo_FullMethodName       = "/peerdb_route.FlowService/GetSlotInfo"
	FlowService_GetStatInfo_FullMethodName       = "/peerdb_route.FlowService/GetStatInfo"
	FlowService_ShutdownFlow_FullMethodName      = "/peerdb_route.FlowService/ShutdownFlow"
	FlowService_FlowStateChange_FullMethodName   = "/peerdb_route.FlowService/FlowStateChange"
	FlowService_MirrorStatus_FullMethodName      = "/peerdb_route.FlowService/MirrorStatus"
)

// FlowServiceClient is the client API for FlowService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type FlowServiceClient interface {
	ValidatePeer(ctx context.Context, in *ValidatePeerRequest, opts ...grpc.CallOption) (*ValidatePeerResponse, error)
	CreatePeer(ctx context.Context, in *CreatePeerRequest, opts ...grpc.CallOption) (*CreatePeerResponse, error)
	DropPeer(ctx context.Context, in *DropPeerRequest, opts ...grpc.CallOption) (*DropPeerResponse, error)
	CreateCDCFlow(ctx context.Context, in *CreateCDCFlowRequest, opts ...grpc.CallOption) (*CreateCDCFlowResponse, error)
	CreateQRepFlow(ctx context.Context, in *CreateQRepFlowRequest, opts ...grpc.CallOption) (*CreateQRepFlowResponse, error)
	GetSchemas(ctx context.Context, in *PostgresPeerActivityInfoRequest, opts ...grpc.CallOption) (*PeerSchemasResponse, error)
	GetTablesInSchema(ctx context.Context, in *SchemaTablesRequest, opts ...grpc.CallOption) (*SchemaTablesResponse, error)
	GetColumns(ctx context.Context, in *TableColumnsRequest, opts ...grpc.CallOption) (*TableColumnsResponse, error)
	GetSlotInfo(ctx context.Context, in *PostgresPeerActivityInfoRequest, opts ...grpc.CallOption) (*PeerSlotResponse, error)
	GetStatInfo(ctx context.Context, in *PostgresPeerActivityInfoRequest, opts ...grpc.CallOption) (*PeerStatResponse, error)
	ShutdownFlow(ctx context.Context, in *ShutdownRequest, opts ...grpc.CallOption) (*ShutdownResponse, error)
	FlowStateChange(ctx context.Context, in *FlowStateChangeRequest, opts ...grpc.CallOption) (*FlowStateChangeResponse, error)
	MirrorStatus(ctx context.Context, in *MirrorStatusRequest, opts ...grpc.CallOption) (*MirrorStatusResponse, error)
}

type flowServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewFlowServiceClient(cc grpc.ClientConnInterface) FlowServiceClient {
	return &flowServiceClient{cc}
}

func (c *flowServiceClient) ValidatePeer(ctx context.Context, in *ValidatePeerRequest, opts ...grpc.CallOption) (*ValidatePeerResponse, error) {
	out := new(ValidatePeerResponse)
	err := c.cc.Invoke(ctx, FlowService_ValidatePeer_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) CreatePeer(ctx context.Context, in *CreatePeerRequest, opts ...grpc.CallOption) (*CreatePeerResponse, error) {
	out := new(CreatePeerResponse)
	err := c.cc.Invoke(ctx, FlowService_CreatePeer_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) DropPeer(ctx context.Context, in *DropPeerRequest, opts ...grpc.CallOption) (*DropPeerResponse, error) {
	out := new(DropPeerResponse)
	err := c.cc.Invoke(ctx, FlowService_DropPeer_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) CreateCDCFlow(ctx context.Context, in *CreateCDCFlowRequest, opts ...grpc.CallOption) (*CreateCDCFlowResponse, error) {
	out := new(CreateCDCFlowResponse)
	err := c.cc.Invoke(ctx, FlowService_CreateCDCFlow_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) CreateQRepFlow(ctx context.Context, in *CreateQRepFlowRequest, opts ...grpc.CallOption) (*CreateQRepFlowResponse, error) {
	out := new(CreateQRepFlowResponse)
	err := c.cc.Invoke(ctx, FlowService_CreateQRepFlow_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) GetSchemas(ctx context.Context, in *PostgresPeerActivityInfoRequest, opts ...grpc.CallOption) (*PeerSchemasResponse, error) {
	out := new(PeerSchemasResponse)
	err := c.cc.Invoke(ctx, FlowService_GetSchemas_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) GetTablesInSchema(ctx context.Context, in *SchemaTablesRequest, opts ...grpc.CallOption) (*SchemaTablesResponse, error) {
	out := new(SchemaTablesResponse)
	err := c.cc.Invoke(ctx, FlowService_GetTablesInSchema_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) GetColumns(ctx context.Context, in *TableColumnsRequest, opts ...grpc.CallOption) (*TableColumnsResponse, error) {
	out := new(TableColumnsResponse)
	err := c.cc.Invoke(ctx, FlowService_GetColumns_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) GetSlotInfo(ctx context.Context, in *PostgresPeerActivityInfoRequest, opts ...grpc.CallOption) (*PeerSlotResponse, error) {
	out := new(PeerSlotResponse)
	err := c.cc.Invoke(ctx, FlowService_GetSlotInfo_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) GetStatInfo(ctx context.Context, in *PostgresPeerActivityInfoRequest, opts ...grpc.CallOption) (*PeerStatResponse, error) {
	out := new(PeerStatResponse)
	err := c.cc.Invoke(ctx, FlowService_GetStatInfo_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) ShutdownFlow(ctx context.Context, in *ShutdownRequest, opts ...grpc.CallOption) (*ShutdownResponse, error) {
	out := new(ShutdownResponse)
	err := c.cc.Invoke(ctx, FlowService_ShutdownFlow_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) FlowStateChange(ctx context.Context, in *FlowStateChangeRequest, opts ...grpc.CallOption) (*FlowStateChangeResponse, error) {
	out := new(FlowStateChangeResponse)
	err := c.cc.Invoke(ctx, FlowService_FlowStateChange_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *flowServiceClient) MirrorStatus(ctx context.Context, in *MirrorStatusRequest, opts ...grpc.CallOption) (*MirrorStatusResponse, error) {
	out := new(MirrorStatusResponse)
	err := c.cc.Invoke(ctx, FlowService_MirrorStatus_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FlowServiceServer is the server API for FlowService service.
// All implementations must embed UnimplementedFlowServiceServer
// for forward compatibility
type FlowServiceServer interface {
	ValidatePeer(context.Context, *ValidatePeerRequest) (*ValidatePeerResponse, error)
	CreatePeer(context.Context, *CreatePeerRequest) (*CreatePeerResponse, error)
	DropPeer(context.Context, *DropPeerRequest) (*DropPeerResponse, error)
	CreateCDCFlow(context.Context, *CreateCDCFlowRequest) (*CreateCDCFlowResponse, error)
	CreateQRepFlow(context.Context, *CreateQRepFlowRequest) (*CreateQRepFlowResponse, error)
	GetSchemas(context.Context, *PostgresPeerActivityInfoRequest) (*PeerSchemasResponse, error)
	GetTablesInSchema(context.Context, *SchemaTablesRequest) (*SchemaTablesResponse, error)
	GetColumns(context.Context, *TableColumnsRequest) (*TableColumnsResponse, error)
	GetSlotInfo(context.Context, *PostgresPeerActivityInfoRequest) (*PeerSlotResponse, error)
	GetStatInfo(context.Context, *PostgresPeerActivityInfoRequest) (*PeerStatResponse, error)
	ShutdownFlow(context.Context, *ShutdownRequest) (*ShutdownResponse, error)
	FlowStateChange(context.Context, *FlowStateChangeRequest) (*FlowStateChangeResponse, error)
	MirrorStatus(context.Context, *MirrorStatusRequest) (*MirrorStatusResponse, error)
	mustEmbedUnimplementedFlowServiceServer()
}

// UnimplementedFlowServiceServer must be embedded to have forward compatible implementations.
type UnimplementedFlowServiceServer struct {
}

func (UnimplementedFlowServiceServer) ValidatePeer(context.Context, *ValidatePeerRequest) (*ValidatePeerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ValidatePeer not implemented")
}
func (UnimplementedFlowServiceServer) CreatePeer(context.Context, *CreatePeerRequest) (*CreatePeerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreatePeer not implemented")
}
func (UnimplementedFlowServiceServer) DropPeer(context.Context, *DropPeerRequest) (*DropPeerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropPeer not implemented")
}
func (UnimplementedFlowServiceServer) CreateCDCFlow(context.Context, *CreateCDCFlowRequest) (*CreateCDCFlowResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateCDCFlow not implemented")
}
func (UnimplementedFlowServiceServer) CreateQRepFlow(context.Context, *CreateQRepFlowRequest) (*CreateQRepFlowResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateQRepFlow not implemented")
}
func (UnimplementedFlowServiceServer) GetSchemas(context.Context, *PostgresPeerActivityInfoRequest) (*PeerSchemasResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSchemas not implemented")
}
func (UnimplementedFlowServiceServer) GetTablesInSchema(context.Context, *SchemaTablesRequest) (*SchemaTablesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTablesInSchema not implemented")
}
func (UnimplementedFlowServiceServer) GetColumns(context.Context, *TableColumnsRequest) (*TableColumnsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetColumns not implemented")
}
func (UnimplementedFlowServiceServer) GetSlotInfo(context.Context, *PostgresPeerActivityInfoRequest) (*PeerSlotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSlotInfo not implemented")
}
func (UnimplementedFlowServiceServer) GetStatInfo(context.Context, *PostgresPeerActivityInfoRequest) (*PeerStatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStatInfo not implemented")
}
func (UnimplementedFlowServiceServer) ShutdownFlow(context.Context, *ShutdownRequest) (*ShutdownResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ShutdownFlow not implemented")
}
func (UnimplementedFlowServiceServer) FlowStateChange(context.Context, *FlowStateChangeRequest) (*FlowStateChangeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FlowStateChange not implemented")
}
func (UnimplementedFlowServiceServer) MirrorStatus(context.Context, *MirrorStatusRequest) (*MirrorStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MirrorStatus not implemented")
}
func (UnimplementedFlowServiceServer) mustEmbedUnimplementedFlowServiceServer() {}

// UnsafeFlowServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to FlowServiceServer will
// result in compilation errors.
type UnsafeFlowServiceServer interface {
	mustEmbedUnimplementedFlowServiceServer()
}

func RegisterFlowServiceServer(s grpc.ServiceRegistrar, srv FlowServiceServer) {
	s.RegisterService(&FlowService_ServiceDesc, srv)
}

func _FlowService_ValidatePeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ValidatePeerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).ValidatePeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_ValidatePeer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).ValidatePeer(ctx, req.(*ValidatePeerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_CreatePeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreatePeerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).CreatePeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_CreatePeer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).CreatePeer(ctx, req.(*CreatePeerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_DropPeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DropPeerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).DropPeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_DropPeer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).DropPeer(ctx, req.(*DropPeerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_CreateCDCFlow_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateCDCFlowRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).CreateCDCFlow(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_CreateCDCFlow_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).CreateCDCFlow(ctx, req.(*CreateCDCFlowRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_CreateQRepFlow_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateQRepFlowRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).CreateQRepFlow(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_CreateQRepFlow_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).CreateQRepFlow(ctx, req.(*CreateQRepFlowRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_GetSchemas_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PostgresPeerActivityInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).GetSchemas(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_GetSchemas_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).GetSchemas(ctx, req.(*PostgresPeerActivityInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_GetTablesInSchema_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SchemaTablesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).GetTablesInSchema(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_GetTablesInSchema_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).GetTablesInSchema(ctx, req.(*SchemaTablesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_GetColumns_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TableColumnsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).GetColumns(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_GetColumns_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).GetColumns(ctx, req.(*TableColumnsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_GetSlotInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PostgresPeerActivityInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).GetSlotInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_GetSlotInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).GetSlotInfo(ctx, req.(*PostgresPeerActivityInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_GetStatInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PostgresPeerActivityInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).GetStatInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_GetStatInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).GetStatInfo(ctx, req.(*PostgresPeerActivityInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_ShutdownFlow_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ShutdownRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).ShutdownFlow(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_ShutdownFlow_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).ShutdownFlow(ctx, req.(*ShutdownRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_FlowStateChange_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FlowStateChangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).FlowStateChange(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_FlowStateChange_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).FlowStateChange(ctx, req.(*FlowStateChangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FlowService_MirrorStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MirrorStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FlowServiceServer).MirrorStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FlowService_MirrorStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FlowServiceServer).MirrorStatus(ctx, req.(*MirrorStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// FlowService_ServiceDesc is the grpc.ServiceDesc for FlowService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var FlowService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "peerdb_route.FlowService",
	HandlerType: (*FlowServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ValidatePeer",
			Handler:    _FlowService_ValidatePeer_Handler,
		},
		{
			MethodName: "CreatePeer",
			Handler:    _FlowService_CreatePeer_Handler,
		},
		{
			MethodName: "DropPeer",
			Handler:    _FlowService_DropPeer_Handler,
		},
		{
			MethodName: "CreateCDCFlow",
			Handler:    _FlowService_CreateCDCFlow_Handler,
		},
		{
			MethodName: "CreateQRepFlow",
			Handler:    _FlowService_CreateQRepFlow_Handler,
		},
		{
			MethodName: "GetSchemas",
			Handler:    _FlowService_GetSchemas_Handler,
		},
		{
			MethodName: "GetTablesInSchema",
			Handler:    _FlowService_GetTablesInSchema_Handler,
		},
		{
			MethodName: "GetColumns",
			Handler:    _FlowService_GetColumns_Handler,
		},
		{
			MethodName: "GetSlotInfo",
			Handler:    _FlowService_GetSlotInfo_Handler,
		},
		{
			MethodName: "GetStatInfo",
			Handler:    _FlowService_GetStatInfo_Handler,
		},
		{
			MethodName: "ShutdownFlow",
			Handler:    _FlowService_ShutdownFlow_Handler,
		},
		{
			MethodName: "FlowStateChange",
			Handler:    _FlowService_FlowStateChange_Handler,
		},
		{
			MethodName: "MirrorStatus",
			Handler:    _FlowService_MirrorStatus_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "route.proto",
}
