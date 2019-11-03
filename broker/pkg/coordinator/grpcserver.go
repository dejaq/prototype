package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	derrors "github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	grpc "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = grpc.BrokerServer(&GRPCServer{})

//GRPCListeners Coordinator can listen and react to these calls
type GRPCListeners struct {
	TimelineCreateMessagesListener func(context.Context, []byte, []timeline.Message) []derrors.MessageIDTuple
	TimelineProducerSubscribed     func(context.Context, Producer)
	TimelineConsumerSubscribed     func(context.Context, Consumer)
	TimelineConsumerUnSubscribed   func(context.Context, Consumer)
	TimelineDeleteMessagesListener func(context.Context, []byte, []timeline.Message) []derrors.MessageIDTuple
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	listeners *GRPCListeners
	greeter   *Greeter
}

func NewGRPCServer(listeners *GRPCListeners, greeter *Greeter) *GRPCServer {
	s := GRPCServer{
		listeners: listeners,
		greeter:   greeter,
	}
	return &s
}

func (s *GRPCServer) SetListeners(listeners *GRPCListeners) {
	s.listeners = listeners
}

func (s *GRPCServer) TimelineProducerHandshake(ctx context.Context, req *grpc.TimelineProducerHandshakeRequest) (*flatbuffers.Builder, error) {
	sessionID, err := s.greeter.ProducerHandshake(req)
	if err != nil {
		return nil, err
	}

	var p Producer
	p.Topic = string(req.TopicID())
	p.Cluster = string(req.Cluster())
	p.GroupID = req.ProducerGroupID()
	s.listeners.TimelineProducerSubscribed(ctx, p)

	builder := flatbuffers.NewBuilder(128)
	sessionIDPos := builder.CreateString(sessionID)
	grpc.TimelineProducerHandshakeResponseStart(builder)
	//TODO transform err to gRPC error
	grpc.TimelineProducerHandshakeResponseAddSessionID(builder, sessionIDPos)
	root := grpc.TimelineProducerHandshakeResponseEnd(builder)
	builder.Finish(root)

	return builder, err
}

func (s *GRPCServer) TimelineConsumerHandshake(ctx context.Context, req *grpc.TimelineConsumerHandshakeRequest) (*flatbuffers.Builder, error) {
	sessionID, err := s.greeter.ConsumerHandshake(req)
	if err != nil {
		return nil, err
	}

	var c Consumer
	c.LeaseMs = req.LeaseTimeoutMS()
	c.Topic = string(req.TopicID())
	c.Cluster = string(req.Cluster())
	c.ID = req.ConsumerID()
	s.listeners.TimelineConsumerSubscribed(ctx, c)

	builder := flatbuffers.NewBuilder(128)
	sessionIDPos := builder.CreateString(sessionID)
	grpc.TimelineConsumerHandshakeResponseStart(builder)
	//TODO transform err to gRPC error
	grpc.TimelineConsumerHandshakeResponseAddSessionID(builder, sessionIDPos)
	root := grpc.TimelineConsumerHandshakeResponseEnd(builder)
	builder.Finish(root)

	return builder, err
}

func (s *GRPCServer) TimelineConsume(req *grpc.TimelineConsumeRequest, stream grpc.Broker_TimelineConsumeServer) error {
	buffer, err := s.greeter.ConsumerConnected(string(req.SessionID()))
	if err != nil {
		return err
	}

	defer func() {
		s.greeter.ConsumerDisconnected(string(req.SessionID()))
	}()

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case lease, ok := <-buffer:
			if !ok {
				return nil //channel closed
			}
			builder.Reset()

			msgIDPosition := builder.CreateByteVector(lease.Message.ID)
			bodyPosition := builder.CreateByteVector(lease.Message.Body)
			producerIDPosition := builder.CreateByteVector(lease.Message.ProducerGroupID)
			grpc.TimelinePushLeaseMessageStart(builder)
			grpc.TimelinePushLeaseMessageAddMessageID(builder, msgIDPosition)
			grpc.TimelinePushLeaseMessageAddTimestampMS(builder, lease.Message.TimestampMS)
			grpc.TimelinePushLeaseMessageAddProducerGroupID(builder, producerIDPosition)
			grpc.TimelinePushLeaseMessageAddVersion(builder, lease.Message.Version)
			grpc.TimelinePushLeaseMessageAddBucketID(builder, lease.Message.BucketID)
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
	}
	return nil
}

func (s *GRPCServer) TimelineCreateMessages(stream grpc.Broker_TimelineCreateMessagesServer) error {
	var msgs []timeline.Message
	var request *grpc.TimelineCreateMessageRequest
	var err error
	var errGet error
	var sessionData *grpc.TimelineProducerHandshakeRequest

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
			_ = fmt.Errorf("TimelineCreateMessages client failed err=%s", err.Error())
			break
		}

		if sessionData == nil {
			sessionData, errGet = s.greeter.GetProducerSessionData(request.SessionID())
			if errGet != nil {
				return err
			}
		}

		msgs = append(msgs, timeline.Message{
			ID:              request.IdBytes(),
			TimestampMS:     request.TimeoutMS(),
			Body:            request.BodyBytes(),
			ProducerGroupID: sessionData.ProducerGroupID(),
			Version:         1,
		})
	}

	if sessionData == nil {
		return errors.New("no message with sessionID")
	}

	msgErrors := s.listeners.TimelineCreateMessagesListener(stream.Context(), sessionData.TopicID(), msgs)

	//returns the response to the client
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)

	root := writeTimelineResponse(msgErrors, builder)
	builder.Finish(root)

	err = stream.SendMsg(builder)
	if err != nil {
		_ = fmt.Errorf("TimelineCreateMessages err=%s", err.Error())
	}

	return nil
}
func (s *GRPCServer) TimelineExtendLease(grpc.Broker_TimelineExtendLeaseServer) error {
	return nil
}
func (s *GRPCServer) TimelineRelease(grpc.Broker_TimelineReleaseServer) error {
	return nil
}
func (s *GRPCServer) TimelineDelete(stream grpc.Broker_TimelineDeleteServer) error {
	//TODO check the sessionID

	var err error
	var topicErr error
	var timelineID []byte

	var req *grpc.TimelineDeleteRequest

	var batch []timeline.Message
	for {
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		batch = append(batch, timeline.Message{
			ID:              req.MessageIDBytes(),
			ProducerGroupID: nil,
			LockConsumerID:  nil,
			BucketID:        req.BucketID(),
			Version:         req.Version(),
		})

		if timelineID == nil {
			timelineID, topicErr = s.greeter.GetTopicFor(req.SessionID())

			if topicErr != nil {
				return errors.New("producer missing session")
			}
		}
	}

	builder := flatbuffers.NewBuilder(128)
	var responseErrors []derrors.MessageIDTuple

	//timelineID can be nil when no messages arrived
	if timelineID != nil {
		responseErrors = s.listeners.TimelineDeleteMessagesListener(stream.Context(), timelineID, batch)
	}
	rootPosition := writeTimelineResponse(responseErrors, builder)
	builder.Finish(rootPosition)
	return stream.SendAndClose(builder)
}
func (s *GRPCServer) TimelineCount(context.Context, *grpc.TimelineCountRequest) (*flatbuffers.Builder, error) {
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
