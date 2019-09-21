package coordinator

import (
	"context"
	"time"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
)

type Coordinator struct {
	storage storage.Repository
	ticker  *time.Ticker
}

func NewCoordinator(ctx context.Context, timelineStorage storage.Repository, tick time.Duration, server *GRPCServer) *Coordinator {
	l := Coordinator{
		storage: timelineStorage,
		ticker:  time.NewTicker(tick),
	}

	server.TimelineCreateMessagesListener = l.listenerTimelineCreateMessages

	go func() {
		for range l.ticker.C {
			available, _, _ := timelineStorage.Select(nil, 10, uint64(time.Now().UTC().Unix()))
			if len(available) == 0 {
				continue
			}
			toSend := make([]timeline.PushLeases, len(available))
			for i := range available {
				toSend[i] = timeline.PushLeases{
					ExpirationTimestampMS: uint64(time.Now().UTC().Unix()) + 120,
					ConsumerID:            []byte("42"),
					Message:               timeline.NewLeaseMessage(available[i]),
				}
			}
			err := server.TimelineCreateMessagesPusher(ctx, toSend)
			if err != nil {
				//cancel the leases!
			}
		}
	}()

	return &l
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, msgs []timeline.Message) []storage.MsgErr {
	return c.storage.Insert(ctx, msgs)
}

var _ = grpc.BrokerServer(&GRPCServer{})

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
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

		grpc.TimelinePushLeasePayloadStart(builder)
		grpc.TimelinePushLeasePayloadAddMessage(builder, msgOffset)
		grpc.TimelinePushLeasePayloadEnd(builder)
	}

	//TODO call the grpc - push to stream
	return nil
}
func (s *GRPCServer) TimelineCreateMessages(ctx context.Context, request *grpc.TimelineCreateMessagePayload) (*flatbuffers.Builder, error) {
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
	s.TimelineCreateMessagesListener(ctx, []timeline.Message{msg})

	return nil, nil
}
func (s *GRPCServer) TimelinePushLeases(*grpc.TimelinePushLeasePayload, grpc.Broker_TimelinePushLeasesServer) error {

}
