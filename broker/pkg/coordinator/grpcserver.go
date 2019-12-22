package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
	grpc "github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = grpc.BrokerServer(&GRPCServer{})

//TimelineListeners Coordinator can listen and react to these calls
type TimelineListeners struct {
	ProducerHandshake      func(context.Context, *Producer) (string, error)
	CreateTimeline         func(context.Context, string, timeline.TopicSettings)
	CreateMessagesRequest  func(context.Context, string) (*Producer, error)
	CreateMessagesListener func(context.Context, string, []timeline.Message) []derrors.MessageIDTuple

	GetConsumer          func(ctx context.Context, session string) (*Consumer, error)
	ConsumerHandshake    func(context.Context, *Consumer) (string, error)
	ConsumerConnected    func(context.Context, string) (chan timeline.Lease, error)
	ConsumerDisconnected func(context.Context, string) error

	DeleteRequest          func(context.Context, string) (string, error)
	DeleteMessagesListener func(context.Context, string, []timeline.Message) []derrors.MessageIDTuple
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	listeners *TimelineListeners
}

func NewGRPCServer(listeners *TimelineListeners) *GRPCServer {
	s := GRPCServer{
		listeners: listeners,
	}
	return &s
}

func (s *GRPCServer) SetListeners(listeners *TimelineListeners) {
	s.listeners = listeners
}

func (s *GRPCServer) TimelineProducerHandshake(ctx context.Context, req *grpc.TimelineProducerHandshakeRequest) (*flatbuffers.Builder, error) {
	var p Producer
	p.Topic = string(req.TopicID())
	p.Cluster = string(req.Cluster())
	p.GroupID = req.ProducerGroupID()
	p.ProducerID = string(req.ProducerID())

	sessionID, err := s.listeners.ProducerHandshake(ctx, &p)
	if err != nil {
		return nil, err
	}

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
	var c Consumer
	c.LeaseMs = req.LeaseTimeoutMS()
	c.Topic = string(req.TopicID())
	c.Cluster = string(req.Cluster())
	c.ID = req.ConsumerID()

	sessionID, err := s.listeners.ConsumerHandshake(ctx, &c)
	if err != nil {
		return nil, err
	}

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
	pipeline, err := s.listeners.ConsumerConnected(stream.Context(), string(req.SessionID()))
	if err != nil {
		return err
	}

	defer func() {
		s.listeners.ConsumerDisconnected(stream.Context(), string(req.SessionID()))
	}()

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case lease, ok := <-pipeline:
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

			consumerIDPosition := builder.CreateByteVector(lease.ConsumerID)
			grpc.TimelinePushLeaseResponseStart(builder)
			grpc.TimelinePushLeaseResponseAddMessage(builder, msgOffset)
			grpc.TimelinePushLeaseResponseAddConsumerID(builder, consumerIDPosition)
			grpc.TimelinePushLeaseResponseAddExpirationTSMSUTC(builder, lease.ExpirationTimestampMS)
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

func (s *GRPCServer) TimelineCreate(ctx context.Context, req *grpc.TimelineCreateRequest) (*flatbuffers.Builder, error) {
	var settings timeline.TopicSettings
	settings.BucketCount = req.BucketCount()
	settings.ReplicaCount = req.ReplicaCount()
	settings.ChecksumBodies = req.ChecksumBodies()
	settings.MaxSecondsLease = req.MaxSecondsLease()
	settings.MaxBodySizeBytes = req.MaxBodySizeBytes()
	settings.RQSLimitPerClient = req.RqsLimitPerClient()
	settings.MinimumDriverVersion = req.MinimumDriverVersion()
	settings.MinimumProtocolVersion = req.MinimumProtocolVersion()
	settings.MaxSecondsFutureAllowed = req.MaxSecondsFutureAllowed()

	s.listeners.CreateTimeline(ctx, string(req.Id()), settings)
	builder := flatbuffers.NewBuilder(128)
	grpc.ErrorStart(builder)
	root := grpc.ErrorEnd(builder)
	builder.Finish(root)
	return builder, nil
}

func (s *GRPCServer) TimelineCreateMessages(stream grpc.Broker_TimelineCreateMessagesServer) error {
	var msgs []timeline.Message
	var request *grpc.TimelineCreateMessageRequest
	var err error
	var errGet error
	var producer *Producer

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
			_ = fmt.Errorf("grpc server TimelineCreateMessages client failed err=%s", err.Error())
			break
		}

		if producer == nil {
			producer, errGet = s.listeners.CreateMessagesRequest(stream.Context(), string(request.SessionID()))
			if errGet != nil {
				return err
			}
		}

		msgs = append(msgs, timeline.Message{
			ID:              request.IdBytes(),
			TimestampMS:     request.TimeoutMS(),
			Body:            request.BodyBytes(),
			ProducerGroupID: producer.GroupID,
			Version:         1,
		})
	}

	if producer == nil {
		return errors.New("no message with sessionID")
	}

	msgErrors := s.listeners.CreateMessagesListener(stream.Context(), producer.Topic, msgs)

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
	var timelineID string

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
		consumer, err := s.listeners.GetConsumer(stream.Context(), string(req.SessionID()))
		if err != nil {
			return err
		}

		batch = append(batch, timeline.Message{
			ID:              req.MessageIDBytes(),
			LockConsumerID:  consumer.ID,
			BucketID:        req.BucketID(),
			Version:         req.Version(),
		})

		if timelineID == "" {
			timelineID, topicErr = s.listeners.DeleteRequest(stream.Context(), string(req.SessionID()))

			if topicErr != nil {
				return errors.New("producer missing session")
			}
		}
	}

	builder := flatbuffers.NewBuilder(128)
	var responseErrors []derrors.MessageIDTuple

	//timelineID can be nil when no messages arrived
	if timelineID != "" {
		responseErrors = s.listeners.DeleteMessagesListener(stream.Context(), timelineID, batch)
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
	return grpc.TimelineResponseEnd(builder)
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
