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
type BrokerClient interface{
  Produce(ctx context.Context, 
  	opts... grpc.CallOption) (Broker_ProduceClient, error)  
  Consume(ctx context.Context, 
  	opts... grpc.CallOption) (Broker_ConsumeClient, error)  
}

type brokerClient struct {
  cc *grpc.ClientConn
}

func NewBrokerClient(cc *grpc.ClientConn) BrokerClient {
  return &brokerClient{cc}
}

func (c *brokerClient) Produce(ctx context.Context, 
	opts... grpc.CallOption) (Broker_ProduceClient, error) {
  stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[0], c.cc, "/DejaQ.Broker/Produce", opts...)
  if err != nil { return nil, err }
  x := &brokerProduceClient{stream}
  return x,nil
}

type Broker_ProduceClient interface {
  Send(*flatbuffers.Builder) error
  CloseAndRecv() (*ProduceResponse, error)
  grpc.ClientStream
}

type brokerProduceClient struct{
  grpc.ClientStream
}

func (x *brokerProduceClient) Send(m *flatbuffers.Builder) error {
  return x.ClientStream.SendMsg(m)
}

func (x *brokerProduceClient) CloseAndRecv() (*ProduceResponse, error) {
  if err := x.ClientStream.CloseSend(); err != nil { return nil, err }
  m := new (ProduceResponse)
  if err := x.ClientStream.RecvMsg(m); err != nil { return nil, err }
  return m, nil
}

func (c *brokerClient) Consume(ctx context.Context, 
	opts... grpc.CallOption) (Broker_ConsumeClient, error) {
  stream, err := grpc.NewClientStream(ctx, &_Broker_serviceDesc.Streams[1], c.cc, "/DejaQ.Broker/Consume", opts...)
  if err != nil { return nil, err }
  x := &brokerConsumeClient{stream}
  return x,nil
}

type Broker_ConsumeClient interface {
  Send(*flatbuffers.Builder) error
  Recv() (*Message, error)
  grpc.ClientStream
}

type brokerConsumeClient struct{
  grpc.ClientStream
}

func (x *brokerConsumeClient) Send(m *flatbuffers.Builder) error {
  return x.ClientStream.SendMsg(m)
}

func (x *brokerConsumeClient) Recv() (*Message, error) {
  m := new(Message)
  if err := x.ClientStream.RecvMsg(m); err != nil { return nil, err }
  return m, nil
}

// Server API for Broker service
type BrokerServer interface {
  Produce(Broker_ProduceServer) error  
  Consume(Broker_ConsumeServer) error  
}

func RegisterBrokerServer(s *grpc.Server, srv BrokerServer) {
  s.RegisterService(&_Broker_serviceDesc, srv)
}

func _Broker_Produce_Handler(srv interface{}, stream grpc.ServerStream) error {
  return srv.(BrokerServer).Produce(&brokerProduceServer{stream})
}

type Broker_ProduceServer interface { 
  Recv() (* ProduceRequest, error)
  SendAndClose(* flatbuffers.Builder) error
  grpc.ServerStream
}

type brokerProduceServer struct {
  grpc.ServerStream
}

func (x *brokerProduceServer) Recv() (*ProduceRequest, error) {
  m := new(ProduceRequest)
  if err := x.ServerStream.RecvMsg(m); err != nil { return nil, err }
  return m, nil
}

func (x *brokerProduceServer) SendAndClose(m *flatbuffers.Builder) error {
  return x.ServerStream.SendMsg(m)
}


func _Broker_Consume_Handler(srv interface{}, stream grpc.ServerStream) error {
  return srv.(BrokerServer).Consume(&brokerConsumeServer{stream})
}

type Broker_ConsumeServer interface { 
  Send(* flatbuffers.Builder) error
  Recv() (* Ack, error)
  grpc.ServerStream
}

type brokerConsumeServer struct {
  grpc.ServerStream
}

func (x *brokerConsumeServer) Send(m *flatbuffers.Builder) error {
  return x.ServerStream.SendMsg(m)
}

func (x *brokerConsumeServer) Recv() (*Ack, error) {
  m := new(Ack)
  if err := x.ServerStream.RecvMsg(m); err != nil { return nil, err }
  return m, nil
}


var _Broker_serviceDesc = grpc.ServiceDesc{
  ServiceName: "DejaQ.Broker",
  HandlerType: (*BrokerServer)(nil),
  Methods: []grpc.MethodDesc{
  },
  Streams: []grpc.StreamDesc{
    {
      StreamName: "Produce",
      Handler: _Broker_Produce_Handler, 
      ClientStreams: true,
    },
    {
      StreamName: "Consume",
      Handler: _Broker_Consume_Handler, 
      ServerStreams: true,
      ClientStreams: true,
    },
  },
}

