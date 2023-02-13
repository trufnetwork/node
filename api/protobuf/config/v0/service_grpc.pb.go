// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: kwil/config/v0/service.proto

package configpb

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

// ConfigServiceClient is the client API for ConfigService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ConfigServiceClient interface {
	GetAll(ctx context.Context, in *GetCfgRequest, opts ...grpc.CallOption) (*GetCfgResponse, error)
	GetFunding(ctx context.Context, in *GetFundingCfgRequest, opts ...grpc.CallOption) (*GetFundingCfgResponse, error)
	GetGateway(ctx context.Context, in *GetGatewayCfgRequest, opts ...grpc.CallOption) (*GetGatewayCfgResponse, error)
}

type configServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewConfigServiceClient(cc grpc.ClientConnInterface) ConfigServiceClient {
	return &configServiceClient{cc}
}

func (c *configServiceClient) GetAll(ctx context.Context, in *GetCfgRequest, opts ...grpc.CallOption) (*GetCfgResponse, error) {
	out := new(GetCfgResponse)
	err := c.cc.Invoke(ctx, "/config.ConfigService/GetAll", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configServiceClient) GetFunding(ctx context.Context, in *GetFundingCfgRequest, opts ...grpc.CallOption) (*GetFundingCfgResponse, error) {
	out := new(GetFundingCfgResponse)
	err := c.cc.Invoke(ctx, "/config.ConfigService/GetFunding", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configServiceClient) GetGateway(ctx context.Context, in *GetGatewayCfgRequest, opts ...grpc.CallOption) (*GetGatewayCfgResponse, error) {
	out := new(GetGatewayCfgResponse)
	err := c.cc.Invoke(ctx, "/config.ConfigService/GetGateway", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ConfigServiceServer is the server API for ConfigService service.
// All implementations must embed UnimplementedConfigServiceServer
// for forward compatibility
type ConfigServiceServer interface {
	GetAll(context.Context, *GetCfgRequest) (*GetCfgResponse, error)
	GetFunding(context.Context, *GetFundingCfgRequest) (*GetFundingCfgResponse, error)
	GetGateway(context.Context, *GetGatewayCfgRequest) (*GetGatewayCfgResponse, error)
	mustEmbedUnimplementedConfigServiceServer()
}

// UnimplementedConfigServiceServer must be embedded to have forward compatible implementations.
type UnimplementedConfigServiceServer struct {
}

func (UnimplementedConfigServiceServer) GetAll(context.Context, *GetCfgRequest) (*GetCfgResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAll not implemented")
}
func (UnimplementedConfigServiceServer) GetFunding(context.Context, *GetFundingCfgRequest) (*GetFundingCfgResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFunding not implemented")
}
func (UnimplementedConfigServiceServer) GetGateway(context.Context, *GetGatewayCfgRequest) (*GetGatewayCfgResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetGateway not implemented")
}
func (UnimplementedConfigServiceServer) mustEmbedUnimplementedConfigServiceServer() {}

// UnsafeConfigServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ConfigServiceServer will
// result in compilation errors.
type UnsafeConfigServiceServer interface {
	mustEmbedUnimplementedConfigServiceServer()
}

func RegisterConfigServiceServer(s grpc.ServiceRegistrar, srv ConfigServiceServer) {
	s.RegisterService(&ConfigService_ServiceDesc, srv)
}

func _ConfigService_GetAll_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetCfgRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConfigServiceServer).GetAll(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/config.ConfigService/GetAll",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConfigServiceServer).GetAll(ctx, req.(*GetCfgRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ConfigService_GetFunding_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetFundingCfgRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConfigServiceServer).GetFunding(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/config.ConfigService/GetFunding",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConfigServiceServer).GetFunding(ctx, req.(*GetFundingCfgRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ConfigService_GetGateway_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetGatewayCfgRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConfigServiceServer).GetGateway(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/config.ConfigService/GetGateway",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConfigServiceServer).GetGateway(ctx, req.(*GetGatewayCfgRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ConfigService_ServiceDesc is the grpc.ServiceDesc for ConfigService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ConfigService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "config.ConfigService",
	HandlerType: (*ConfigServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetAll",
			Handler:    _ConfigService_GetAll_Handler,
		},
		{
			MethodName: "GetFunding",
			Handler:    _ConfigService_GetFunding_Handler,
		},
		{
			MethodName: "GetGateway",
			Handler:    _ConfigService_GetGateway_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "kwil/config/v0/service.proto",
}
