package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
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
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	InnerServer *innerServer
}

func NewGRPCServer(listeners *GRPCListeners) *GRPCServer {
	s := GRPCServer{

		InnerServer: &innerServer{
			builderPool: sync.Pool{},
			listeners:   listeners,
		},
	}
	s.InnerServer.builderPool.New = func() interface{} {
		return flatbuffers.NewBuilder(128)
	}
	return &s
}

// TimelineCreateMessagesPusher is used by the coordinator to push msgs when needed
func (s *GRPCServer) pushMessagesToConsumer(ctx context.Context, leases []timeline.PushLeases) error {
	if s.InnerServer.streamToClient == nil {
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
		builder.Reset()

		lease := leases[i]
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
		err := s.InnerServer.streamToClient.Send(builder)
		if err != nil {
			fmt.Errorf("TimelineCreateMessagesPusher err=%s", err.Error())
		}
	}

	return nil
}

type innerServer struct {
	streamToClient grpc.Broker_TimelinePushLeasesServer
	builderPool    sync.Pool
	listeners      *GRPCListeners
}

func (s *innerServer) TimelinePushLeases(req *grpc.TimelinePushLeaseSubscribeRequest, stream grpc.Broker_TimelinePushLeasesServer) error {
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
