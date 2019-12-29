//Generated by gRPC Go plugin
//If you make any local changes, they will be lost
//source: dejaq

package DejaQ

import "github.com/google/flatbuffers/go"

import (
	context "context"
	grpc "google.golang.org/grpc"
)

// Client API for Broker service
type BrokerClient interface {
	TimelineProducerHandshake(ctx context.Context, in *flatbuffers.Builder,
		opts ...grpc.CallOption) (*TimelineProducerHandshakeResponse, error)
	TimelineConsumerHandshake(ctx context.Context, in *flatbuffers.Builder,
		opts ...grpc.CallOption) (*TimelineConsumerHandshakeResponse, error)
	TimelineCreate(ctx context.Context, in *flatbuffers.Builder,
		opts ...grpc.CallOption) (*Error, error)
	TimelineCount(ctx context.Context, in *flatbuffers.Builder,
		opts ...grpc.CallOption) (*TimelineCountResponse, error)
	TimelineConsume(ctx context.Context,
		opts ...grpc.CallOption) (Broker_TimelineConsumeClient, error)
	TimelineCreateMessages(ctx context.Context,
		opts ...grpc.CallOption) (Broker_TimelineCreateMessagesClient, error)
	TimelineExtendLease(ctx context.Context,
		opts ...grpc.CallOption) (Broker_TimelineExtendLeaseClient, error)
	TimelineRelease(ctx context.Context,
		opts ...grpc.CallOption) (Broker_TimelineReleaseClient, error)
	TimelineDelete(ctx context.Context,
		opts ...grpc.CallOption) (Broker_TimelineDeleteClient, error)
}

type brokerClient struct {
	cc *grpc.ClientConn
}

func NewBrokerClient(cc *grpc.ClientConn) BrokerClient {
	return &brokerClient{cc}
}

func (c *brokerClient) TimelineProducerHandshake(ctx context.Context, in *flatbuffers.Builder,
	opts ...grpc.CallOption) (*TimelineProducerHandshakeResponse, error) {
	out := new(TimelineProducerHandshakeResponse)
	err := grpc.Invoke(ctx, "/DejaQ.Broker/TimelineProducerHandshake", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) TimelineConsumerHandshake(ctx context.Context, in *flatbuffers.Builder,
	opts ...grpc.CallOption) (*TimelineConsumerHandshakeResponse, error) {
	out := new(TimelineConsumerHandshakeResponse)
	err := grpc.Invoke(ctx, "/DejaQ.Broker/TimelineConsumerHandshake", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) TimelineCreate(ctx context.Context, in *flatbuffers.Builder,
	opts ...grpc.CallOption) (*Error, error) {
	out := new(Error)
	err := grpc.Invoke(ctx, "/DejaQ.Broker/TimelineCreate", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) TimelineCount(ctx context.Context, in *flatbuffers.Builder,
	opts ...grpc.CallOption) (*TimelineCountResponse, error) {
	out := new(TimelineCountResponse)
	err := grpc.Invoke(ctx, "/DejaQ.Broker/TimelineCount", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *brokerClient) TimelineConsume(ctx context.Context,
	opts ...grpc.CallOption) (Broker_TimelineConsumeClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[0], c.cc, "/DejaQ.Broker/TimelineConsume", opts...)
	if err != nil {
		return nil, err
	}
	x := &brokerTimelineConsumeClient{stream}
	return x, nil
}

type Broker_TimelineConsumeClient interface {
	Send(*flatbuffers.Builder) error
	Recv() (*TimelinePushLeaseResponse, error)
	grpc.ClientStream
}

type brokerTimelineConsumeClient struct {
	grpc.ClientStream
}

func (x *brokerTimelineConsumeClient) Send(m *flatbuffers.Builder) error {
	return x.ClientStream.SendMsg(m)
}

func (x *brokerTimelineConsumeClient) Recv() (*TimelinePushLeaseResponse, error) {
	m := new(TimelinePushLeaseResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *brokerClient) TimelineCreateMessages(ctx context.Context,
	opts ...grpc.CallOption) (Broker_TimelineCreateMessagesClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[1], c.cc, "/DejaQ.Broker/TimelineCreateMessages", opts...)
	if err != nil {
		return nil, err
	}
	x := &brokerTimelineCreateMessagesClient{stream}
	return x, nil
}

type Broker_TimelineCreateMessagesClient interface {
	Send(*flatbuffers.Builder) error
	CloseAndRecv() (*TimelineResponse, error)
	grpc.ClientStream
}

type brokerTimelineCreateMessagesClient struct {
	grpc.ClientStream
}

func (x *brokerTimelineCreateMessagesClient) Send(m *flatbuffers.Builder) error {
	return x.ClientStream.SendMsg(m)
}

func (x *brokerTimelineCreateMessagesClient) CloseAndRecv() (*TimelineResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(TimelineResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *brokerClient) TimelineExtendLease(ctx context.Context,
	opts ...grpc.CallOption) (Broker_TimelineExtendLeaseClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[2], c.cc, "/DejaQ.Broker/TimelineExtendLease", opts...)
	if err != nil {
		return nil, err
	}
	x := &brokerTimelineExtendLeaseClient{stream}
	return x, nil
}

type Broker_TimelineExtendLeaseClient interface {
	Send(*flatbuffers.Builder) error
	CloseAndRecv() (*TimelineResponse, error)
	grpc.ClientStream
}

type brokerTimelineExtendLeaseClient struct {
	grpc.ClientStream
}

func (x *brokerTimelineExtendLeaseClient) Send(m *flatbuffers.Builder) error {
	return x.ClientStream.SendMsg(m)
}

func (x *brokerTimelineExtendLeaseClient) CloseAndRecv() (*TimelineResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(TimelineResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *brokerClient) TimelineRelease(ctx context.Context,
	opts ...grpc.CallOption) (Broker_TimelineReleaseClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[3], c.cc, "/DejaQ.Broker/TimelineRelease", opts...)
	if err != nil {
		return nil, err
	}
	x := &brokerTimelineReleaseClient{stream}
	return x, nil
}

type Broker_TimelineReleaseClient interface {
	Send(*flatbuffers.Builder) error
	CloseAndRecv() (*TimelineResponse, error)
	grpc.ClientStream
}

type brokerTimelineReleaseClient struct {
	grpc.ClientStream
}

func (x *brokerTimelineReleaseClient) Send(m *flatbuffers.Builder) error {
	return x.ClientStream.SendMsg(m)
}

func (x *brokerTimelineReleaseClient) CloseAndRecv() (*TimelineResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(TimelineResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *brokerClient) TimelineDelete(ctx context.Context,
	opts ...grpc.CallOption) (Broker_TimelineDeleteClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[4], c.cc, "/DejaQ.Broker/TimelineDelete", opts...)
	if err != nil {
		return nil, err
	}
	x := &brokerTimelineDeleteClient{stream}
	return x, nil
}

type Broker_TimelineDeleteClient interface {
	Send(*flatbuffers.Builder) error
	CloseAndRecv() (*TimelineResponse, error)
	grpc.ClientStream
}

type brokerTimelineDeleteClient struct {
	grpc.ClientStream
}

func (x *brokerTimelineDeleteClient) Send(m *flatbuffers.Builder) error {
	return x.ClientStream.SendMsg(m)
}

func (x *brokerTimelineDeleteClient) CloseAndRecv() (*TimelineResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(TimelineResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for Broker service
type BrokerServer interface {
	TimelineProducerHandshake(context.Context, *TimelineProducerHandshakeRequest) (*flatbuffers.Builder, error)
	TimelineConsumerHandshake(context.Context, *TimelineConsumerHandshakeRequest) (*flatbuffers.Builder, error)
	TimelineCreate(context.Context, *TimelineCreateRequest) (*flatbuffers.Builder, error)
	TimelineCount(context.Context, *TimelineCountRequest) (*flatbuffers.Builder, error)
	TimelineConsume(Broker_TimelineConsumeServer) error
	TimelineCreateMessages(Broker_TimelineCreateMessagesServer) error
	TimelineExtendLease(Broker_TimelineExtendLeaseServer) error
	TimelineRelease(Broker_TimelineReleaseServer) error
	TimelineDelete(Broker_TimelineDeleteServer) error
}

func RegisterBrokerServer(s *grpc.Server, srv BrokerServer) {
	s.RegisterService(&_Broker_serviceDesc, srv)
}

func _Broker_TimelineProducerHandshake_Handler(srv interface{}, ctx context.Context,
	dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TimelineProducerHandshakeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).TimelineProducerHandshake(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/DejaQ.Broker/TimelineProducerHandshake",
	}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).TimelineProducerHandshake(ctx, req.(*TimelineProducerHandshakeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_TimelineConsumerHandshake_Handler(srv interface{}, ctx context.Context,
	dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TimelineConsumerHandshakeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).TimelineConsumerHandshake(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/DejaQ.Broker/TimelineConsumerHandshake",
	}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).TimelineConsumerHandshake(ctx, req.(*TimelineConsumerHandshakeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_TimelineCreate_Handler(srv interface{}, ctx context.Context,
	dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TimelineCreateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).TimelineCreate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/DejaQ.Broker/TimelineCreate",
	}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).TimelineCreate(ctx, req.(*TimelineCreateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_TimelineCount_Handler(srv interface{}, ctx context.Context,
	dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TimelineCountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BrokerServer).TimelineCount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/DejaQ.Broker/TimelineCount",
	}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BrokerServer).TimelineCount(ctx, req.(*TimelineCountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Broker_TimelineConsume_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(BrokerServer).TimelineConsume(&brokerTimelineConsumeServer{stream})
}

type Broker_TimelineConsumeServer interface {
	Send(*flatbuffers.Builder) error
	Recv() (*TimelineConsumerStatus, error)
	grpc.ServerStream
}

type brokerTimelineConsumeServer struct {
	grpc.ServerStream
}

func (x *brokerTimelineConsumeServer) Send(m *flatbuffers.Builder) error {
	return x.ServerStream.SendMsg(m)
}

func (x *brokerTimelineConsumeServer) Recv() (*TimelineConsumerStatus, error) {
	m := new(TimelineConsumerStatus)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Broker_TimelineCreateMessages_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(BrokerServer).TimelineCreateMessages(&brokerTimelineCreateMessagesServer{stream})
}

type Broker_TimelineCreateMessagesServer interface {
	Recv() (*TimelineCreateMessageRequest, error)
	SendAndClose(*flatbuffers.Builder) error
	grpc.ServerStream
}

type brokerTimelineCreateMessagesServer struct {
	grpc.ServerStream
}

func (x *brokerTimelineCreateMessagesServer) Recv() (*TimelineCreateMessageRequest, error) {
	m := new(TimelineCreateMessageRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *brokerTimelineCreateMessagesServer) SendAndClose(m *flatbuffers.Builder) error {
	return x.ServerStream.SendMsg(m)
}

func _Broker_TimelineExtendLease_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(BrokerServer).TimelineExtendLease(&brokerTimelineExtendLeaseServer{stream})
}

type Broker_TimelineExtendLeaseServer interface {
	Recv() (*TimelineExtendLeaseRequest, error)
	SendAndClose(*flatbuffers.Builder) error
	grpc.ServerStream
}

type brokerTimelineExtendLeaseServer struct {
	grpc.ServerStream
}

func (x *brokerTimelineExtendLeaseServer) Recv() (*TimelineExtendLeaseRequest, error) {
	m := new(TimelineExtendLeaseRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *brokerTimelineExtendLeaseServer) SendAndClose(m *flatbuffers.Builder) error {
	return x.ServerStream.SendMsg(m)
}

func _Broker_TimelineRelease_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(BrokerServer).TimelineRelease(&brokerTimelineReleaseServer{stream})
}

type Broker_TimelineReleaseServer interface {
	Recv() (*TimelineReleaseRequest, error)
	SendAndClose(*flatbuffers.Builder) error
	grpc.ServerStream
}

type brokerTimelineReleaseServer struct {
	grpc.ServerStream
}

func (x *brokerTimelineReleaseServer) Recv() (*TimelineReleaseRequest, error) {
	m := new(TimelineReleaseRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *brokerTimelineReleaseServer) SendAndClose(m *flatbuffers.Builder) error {
	return x.ServerStream.SendMsg(m)
}

func _Broker_TimelineDelete_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(BrokerServer).TimelineDelete(&brokerTimelineDeleteServer{stream})
}

type Broker_TimelineDeleteServer interface {
	Recv() (*TimelineDeleteRequest, error)
	SendAndClose(*flatbuffers.Builder) error
	grpc.ServerStream
}

type brokerTimelineDeleteServer struct {
	grpc.ServerStream
}

func (x *brokerTimelineDeleteServer) Recv() (*TimelineDeleteRequest, error) {
	m := new(TimelineDeleteRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *brokerTimelineDeleteServer) SendAndClose(m *flatbuffers.Builder) error {
	return x.ServerStream.SendMsg(m)
}

var _Broker_serviceDesc = grpc.ServiceDesc{
	ServiceName: "DejaQ.Broker",
	HandlerType: (*BrokerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "TimelineProducerHandshake",
			Handler:    _Broker_TimelineProducerHandshake_Handler,
		},
		{
			MethodName: "TimelineConsumerHandshake",
			Handler:    _Broker_TimelineConsumerHandshake_Handler,
		},
		{
			MethodName: "TimelineCreate",
			Handler:    _Broker_TimelineCreate_Handler,
		},
		{
			MethodName: "TimelineCount",
			Handler:    _Broker_TimelineCount_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "TimelineConsume",
			Handler:       _Broker_TimelineConsume_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "TimelineCreateMessages",
			Handler:       _Broker_TimelineCreateMessages_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "TimelineExtendLease",
			Handler:       _Broker_TimelineExtendLease_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "TimelineRelease",
			Handler:       _Broker_TimelineRelease_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "TimelineDelete",
			Handler:       _Broker_TimelineDelete_Handler,
			ClientStreams: true,
		},
	},
}
