package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	derrors "github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = grpc.BrokerServer(&innerServer{})

//GRPCListeners Coordinator can listen and react to these calls
type GRPCListeners struct {
	TimelineCreateMessagesListener func(context.Context, []timeline.Message) []derrors.MessageIDTuple
	TimelineConsumerSubscribed     func(context.Context, Consumer)
	TimelineConsumerUnSubscribed   func(context.Context, Consumer)
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	InnerServer *innerServer
}

func NewGRPCServer(listeners *GRPCListeners) *GRPCServer {
	s := GRPCServer{

		InnerServer: &innerServer{
			builderPool:        sync.Pool{},
			listeners:          listeners,
			consumers:          sync.Map{},
			consumerBufferSize: 50,
		},
	}
	s.InnerServer.builderPool.New = func() interface{} {
		return flatbuffers.NewBuilder(128)
	}
	return &s
}

// TimelineCreateMessagesPusher is used by the coordinator to push msgs when needed
func (s *GRPCServer) pushMessagesToConsumer(ctx context.Context, consumerID []byte, leases []timeline.PushLeases) error {
	asInterface, _ := s.InnerServer.consumers.Load(string(consumerID))
	buffer := asInterface.(chan timeline.PushLeases)

	for i := range leases {
		buffer <- leases[i]
	}
	return nil
}

type innerServer struct {
	builderPool        sync.Pool
	listeners          *GRPCListeners
	consumers          sync.Map
	consumerBufferSize int
}

func (s *innerServer) TimelineConsumerHandshake(req *grpc.TimelineConsumerHandshakeRequest, stream grpc.Broker_TimelineConsumerHandshakeServer) error {

	var c Consumer
	c.LeaseMs = req.LeaseTimeoutMS()
	c.Topic = string(req.TopicID())
	c.Cluster = string(req.Cluster())
	c.ID = req.ConsumerIDBytes()
	s.listeners.TimelineConsumerSubscribed(context.Background(), c)

	leasesPipeline := make(chan timeline.PushLeases, s.consumerBufferSize)
	s.consumers.Store(string(c.ID), leasesPipeline)

	//builder = s.builderPool.Get().(*flatbuffers.Builder)
	//defer func() {
	//	builder.Reset()
	//	s.builderPool.Put(builder)
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	for lease := range leasesPipeline {
		builder.Reset()

		msgIDPosition := builder.CreateByteVector(lease.Message.ID)
		bodyPosition := builder.CreateByteVector(lease.Message.Body)
		producerIDPosition := builder.CreateByteVector(lease.Message.ProducerGroupID)
		grpc.TimelinePushLeaseMessageStart(builder)
		grpc.TimelinePushLeaseMessageAddMessageID(builder, msgIDPosition)
		grpc.TimelinePushLeaseMessageAddTimestampMS(builder, lease.Message.TimestampMS)
		grpc.TimelinePushLeaseMessageAddProducerGroupID(builder, producerIDPosition)
		grpc.TimelinePushLeaseMessageAddVersion(builder, lease.Message.Version)
		grpc.TimelinePushLeaseMessageAddBody(builder, bodyPosition)
		msgOffset := grpc.TimelinePushLeaseMessageEnd(builder)

		grpc.TimelinePushLeaseResponseStart(builder)
		grpc.TimelinePushLeaseResponseAddMessage(builder, msgOffset)
		rootPosition := grpc.TimelinePushLeaseResponseEnd(builder)

		builder.Finish(rootPosition)
		if err := stream.Send(builder); err != nil {
			log.Fatalf("loader failed: %s", err.Error())
			return err
		}
	}

	return errors.New("coordinator closed this consumer")
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

		msgErrors := s.listeners.TimelineCreateMessagesListener(context.Background(), msgs)

		//returns the response to the client
		var builder *flatbuffers.Builder
		builder = flatbuffers.NewBuilder(128)

		root := writeTimelineResponse(msgErrors, builder)
		builder.Finish(root)

		err = stream.SendMsg(builder)
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

func writeTimelineResponse(errors []derrors.MessageIDTuple, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	var rootListErrors flatbuffers.UOffsetT
	if len(errors) == 0 {
		return rootListErrors
	}
	for i := range errors {
		messageIDRoot := builder.CreateByteVector(errors[i].MessageID)
		errorRoot := writeError(errors[i].Error, builder)
		grpc.TimelineMessageIDErrorTupleStart(builder)
		grpc.TimelineMessageIDErrorTupleAddMessgeID(builder, messageIDRoot)
		grpc.TimelineMessageIDErrorTupleAddErr(builder, errorRoot)
		rootListErrors = grpc.TimelineMessageIDErrorTupleEnd(builder)
	}

	grpc.TimelineResponseStart(builder)
	grpc.TimelineResponseAddMessagesErrors(builder, rootListErrors)
	grpc.TimelineResponseEnd(builder)

	return grpc.ErrorEnd(builder)
}

func writeError(err derrors.Dejaror, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	//TODO first add the details tuples (map to tuples)

	messageRoot := builder.CreateString(err.Message)
	opRoot := builder.CreateString(err.Operation.String())
	grpc.ErrorStart(builder)
	grpc.ErrorAddMessage(builder, messageRoot)
	grpc.ErrorAddOp(builder, opRoot)
	grpc.ErrorAddKind(builder, uint64(err.Kind))
	grpc.ErrorAddSeverity(builder, uint16(err.Severity))
	grpc.ErrorAddShouldRetry(builder, err.ShouldRetry)
	grpc.ErrorAddShouldSync(builder, err.ClientShouldSync)
	grpc.ErrorAddModule(builder, uint8(err.Module))
	return grpc.ErrorEnd(builder)
}
