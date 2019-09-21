package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = grpc.BrokerServer(&innerServer{})

//GRPCListeners Coordinator can listen and react to these calls
type GRPCListeners struct {
	TimelineCreateMessagesListener func(context.Context, []timeline.Message) []storage.MsgErr
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	innerServer *innerServer
}

func NewGRPCServer(listeners *GRPCListeners) *GRPCServer {
	s := GRPCServer{

		innerServer: &innerServer{
			builderPool: sync.Pool{},
			listeners:   listeners,
		},
	}
	s.innerServer.builderPool.New = func() interface{} {
		return flatbuffers.NewBuilder(128)
	}
	return &s
}

// TimelineCreateMessagesPusher is used by the coordinator to push msgs when needed
func (s *GRPCServer) pushMessagesToConsumer(ctx context.Context, leases []timeline.PushLeases) error {
	if s.innerServer == nil {
		return errors.New("client is not yet subscribed, call TimelinePushLeases")
	}
	var builder *flatbuffers.Builder
	//TODO check with the flatb/grpc if is safe to reuse these with defer or we need to wait for an async operation ?!
	//builder = s.builderPool.Get().(*flatbuffers.Builder)
	//defer func() {
	//	builder.Reset()
	//	s.builderPool.Put(builder)
	//}()
	builder = flatbuffers.NewBuilder(128)

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

	err := s.innerServer.streamToClient.Send(builder)
	if err != nil {
		fmt.Errorf("TimelineCreateMessagesPusher err=%s", err.Error())
	}
	return nil
}

type innerServer struct {
	streamToClient grpc.Broker_TimelinePushLeasesServer
	builderPool    sync.Pool
	listeners      *GRPCListeners
}

func (s *innerServer) TimelinePushLeases(req *grpc.TimelinePushLeaseRequest, stream grpc.Broker_TimelinePushLeasesServer) error {
	//TODO this is a temporary subscribe to the topic, we keep its stream here open
	s.streamToClient = stream
	return nil
}
func (s *innerServer) TimelineCreateMessages(stream grpc.Broker_TimelineCreateMessagesServer) error {
	go func(stream grpc.Broker_TimelineCreateMessagesServer) {
		var msgs []timeline.Message
		var request *grpc.TimelineCreateMessageRequest
		var err error

		//gather all the messages from the client
		for err == nil {
			request, err = stream.Recv()
			if request == nil { //empty msg ?!?!?! TODO log this as a warning
				continue
			}
			if err == io.EOF { //it means the stream batch is over
				break
			}
			if err != nil {
				fmt.Errorf("TimelineCreateMessages client failed err=%s", err.Error())
				break
			}

			msgs = append(msgs, timeline.Message{
				ID:              request.IdBytes(),
				TimestampMS:     request.TimeoutMS(),
				BodyID:          nil,
				Body:            request.BodyBytes(),
				ProducerGroupID: nil, //TODO populate with the data from handshake
				LockConsumerID:  nil,
				BucketID:        0,
				Version:         1,
			})
		}

		errors := s.listeners.TimelineCreateMessagesListener(context.Background(), msgs)

		//returns the response to the client
		err = stream.SendMsg(errors)
		if err != nil {
			fmt.Errorf("TimelineCreateMessages err=%s", err.Error())
		}
	}(stream)

	return nil
}
func (s *innerServer) TimelineExtendLease(grpc.Broker_TimelineExtendLeaseServer) error {
	return nil
}
func (s *innerServer) TimelineRelease(grpc.Broker_TimelineReleaseServer) error {
	return nil
}
func (s *innerServer) TimelineDelete(grpc.Broker_TimelineDeleteServer) error {
	return nil
}
func (s *innerServer) TimelineCount(context.Context, *grpc.TimelineCountRequest) (*flatbuffers.Builder, error) {
	return nil, nil
}
