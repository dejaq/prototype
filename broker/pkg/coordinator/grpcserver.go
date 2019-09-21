package coordinator

import (
	"context"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = grpc.BrokerServer(&GRPCServer{})

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	streamToClient grpc.Broker_TimelinePushLeasesServer
	//Coordinator can listen and react to these calls
	TimelineCreateMessagesListener func(context.Context, []timeline.Message) []storage.MsgErr
}

// TimelineCreateMessagesPusher is used by the coordinator to push msgs when needed
func (s *GRPCServer) TimelineCreateMessagesPusher(ctx context.Context, leases []timeline.PushLeases) error {
	builder := flatbuffers.NewBuilder(0)

	for i := range leases {
		lease := leases[i]
		grpc.TimelinePushLeaseMessageStart(builder)
		grpc.TimelinePushLeaseMessageAddMessageID(builder, builder.CreateByteVector(lease.Message.ID))
		grpc.TimelinePushLeaseMessageAddTimestampMS(builder, lease.Message.TimestampMS)
		grpc.TimelinePushLeaseMessageAddProducerGroupID(builder, builder.CreateByteVector(lease.Message.ProducerGroupID))
		grpc.TimelinePushLeaseMessageAddVersion(builder, lease.Message.Version)
		grpc.TimelinePushLeaseMessageAddBody(builder, builder.CreateByteVector(lease.Message.Body))
		msgOffset := grpc.TimelinePushLeaseMessageEnd(builder)

		grpc.TimelinePushLeaseRequestStart(builder)
		grpc.TimelinePushLeaseRequestAddMessage(builder, msgOffset)
		grpc.TimelinePushLeaseRequestEnd(builder)
	}

	s.streamToClient.Send(builder)
	return nil
}

func (s *GRPCServer) TimelinePushLeases(req *grpc.TimelinePushLeaseRequest, stream grpc.Broker_TimelinePushLeasesServer) error {
	s.streamToClient = stream
	return nil
}
func (s *GRPCServer) TimelineCreateMessages(stream grpc.Broker_TimelineCreateMessagesServer) error {
	request, _ := stream.Recv()

	msg := timeline.Message{
		ID:              request.IdBytes(),
		TimestampMS:     request.TimeoutMS(),
		BodyID:          nil,
		Body:            request.BodyBytes(),
		ProducerGroupID: nil,
		LockConsumerID:  nil,
		BucketID:        0,
		Version:         0,
	}
	s.TimelineCreateMessagesListener(context.Background(), []timeline.Message{msg})

	return nil
}
func (s *GRPCServer) TimelineExtendLease(grpc.Broker_TimelineExtendLeaseServer) error {
	return nil
}
func (s *GRPCServer) TimelineRelease(grpc.Broker_TimelineReleaseServer) error {
	return nil
}
func (s *GRPCServer) TimelineDelete(grpc.Broker_TimelineDeleteServer) error {
	return nil
}
func (s *GRPCServer) TimelineCount(context.Context, *grpc.TimelineCountRequest) (*flatbuffers.Builder, error) {
	return nil, nil
}
