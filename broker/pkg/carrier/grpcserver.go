package carrier

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dejaq/prototype/common/metrics/exporter"

	"github.com/dejaq/prototype/common/protocol"
	"github.com/sirupsen/logrus"

	"io"

	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
	grpc "github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
)

var _ = grpc.BrokerServer(&GRPCServer{})

var (
	//metricTopicCommandsCounter = exporter.GetBrokerCounter("topic_commands_count", []string{"operation"})
	//metricTopicCommandsErrors  = exporter.GetBrokerCounter("topic_commands_errors", []string{"operation", "Topic"})

	//count of messages that are created or removed
	metricMessagesCounterSuccess = exporter.GetBrokerCounter("topic_messages_count", []string{"operation", "Topic"})
	metricMessagesCounterError   = exporter.GetBrokerCounter("topic_messages_errors", []string{"operation", "Topic"})
	//number of leases (1 lease == 1 msg)
	metricTopicLeasesCounter = exporter.GetBrokerGauge("topic_leases_count", []string{"operation", "Topic"})
	metricTopicLeasesErrors  = exporter.GetBrokerCounter("topic_leases_errors", []string{"operation", "Topic"})
)

//TimelineListeners Coordinator can listen and react to these calls
//TODO transform this in an interface!
type TimelineListeners struct {
	ProducerHandshake     func(context.Context, *Producer) (string, error)
	CreateTimeline        func(context.Context, string, timeline.TopicSettings) error
	CreateMessagesRequest func(context.Context, string) (*Producer, error)
	// CreateMessagesListener Returns a Dejaror or a MessageIDTupleList (if only specific messages failed)
	CreateMessagesListener func(context.Context, timeline.InsertMessagesRequest) error

	ConsumerHandshake    func(context.Context, *Consumer) (string, error)
	ConsumerStatus       func(context.Context, protocol.ConsumerStatus) error
	ConsumerConnected    func(context.Context, string) (*ConsumerPipelineTuple, error)
	ConsumerDisconnected func(context.Context, string) error

	// Returns a Dejaror or a MessageIDTupleList (if only specific messages failed)
	DeleteRequest          func(context.Context, string) (string, error)
	DeleteMessagesListener func(context.Context, string, timeline.DeleteMessagesRequest) error
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	listeners *TimelineListeners
	logger    logrus.FieldLogger
}

func NewGRPCServer(listeners *TimelineListeners) *GRPCServer {
	s := GRPCServer{
		listeners: listeners,
		logger:    logrus.New().WithField("component", "grpcServer"),
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
	p.GroupID = string(req.ProducerGroupID())
	p.ProducerID = string(req.ProducerID())

	sessionID, err := s.listeners.ProducerHandshake(ctx, &p)
	builder := flatbuffers.NewBuilder(128)
	var errPosition flatbuffers.UOffsetT
	if err != nil {
		errPosition = writeError(err, builder)
	}

	sessionIDPos := builder.CreateString(sessionID)
	grpc.TimelineProducerHandshakeResponseStart(builder)
	if err != nil {
		grpc.TimelineProducerHandshakeResponseAddErr(builder, errPosition)
	}
	grpc.TimelineProducerHandshakeResponseAddSessionID(builder, sessionIDPos)
	root := grpc.TimelineProducerHandshakeResponseEnd(builder)
	builder.Finish(root)

	return builder, err
}

func (s *GRPCServer) TimelineConsumerHandshake(ctx context.Context, req *grpc.TimelineConsumerHandshakeRequest) (*flatbuffers.Builder, error) {
	c := NewConsumer(req.ConsumerID(), string(req.TopicID()), string(req.Cluster()), req.LeaseTimeoutMS())

	sessionID, err := s.listeners.ConsumerHandshake(ctx, c)
	builder := flatbuffers.NewBuilder(128)
	var errPosition flatbuffers.UOffsetT
	if err != nil {
		errPosition = writeError(err, builder)
	}

	sessionIDPos := builder.CreateString(sessionID)
	grpc.TimelineConsumerHandshakeResponseStart(builder)
	if err != nil {
		grpc.TimelineProducerHandshakeResponseAddErr(builder, errPosition)
	}
	grpc.TimelineConsumerHandshakeResponseAddSessionID(builder, sessionIDPos)
	root := grpc.TimelineConsumerHandshakeResponseEnd(builder)
	builder.Finish(root)

	return builder, err
}

func (s *GRPCServer) TimelineConsume(stream grpc.Broker_TimelineConsumeServer) error {
	sessionChan := make(chan []byte)
	go func() {
		var err error
		firstRun := true
		var request *grpc.TimelineConsumerStatus
		for err == nil {
			select {
			case <-stream.Context().Done():
				return
			default:
				request, err = stream.Recv()
				if request == nil { //empty status ?!?!?! TODO log this as a warning
					continue
				}
				if err == io.EOF { //it means the stream batch is over
					break
				}
				if err != nil {
					_ = fmt.Errorf("grpc server TimelineCreateMessages client failed err=%s", err.Error())
					break
				}
				if firstRun {
					firstRun = false
					sessionChan <- request.SessionID()
				}

				consumerStatus := protocol.NewConsumerStatus(request.SessionID())
				consumerStatus.AvailableBufferSize = request.AvailableBufferSize()
				consumerStatus.MaxBufferSize = request.MaxBufferSize()
				err = s.listeners.ConsumerStatus(stream.Context(), consumerStatus)
				if err != nil {
					logrus.Errorf("Update Consumer status failed, err=%s", err)
				}
			}
		}
	}()

	var sessionID []byte
	select {
	case sessionID = <-sessionChan:
		close(sessionChan)
	case <-time.After(time.Minute):
		msg := "timeout exceeded waiting for consumer first status update"
		logrus.Error(msg)
		return errors.New(msg)
	}

	consumerTuple, err := s.listeners.ConsumerConnected(stream.Context(), string(sessionID))
	if err != nil {
		return err
	}

	defer func() {
		s.listeners.ConsumerDisconnected(stream.Context(), string(sessionID))
	}()

	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case lease, ok := <-consumerTuple.Pipeline:
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
				logrus.Errorf("loader failed: %s", err.Error())
				metricTopicLeasesErrors.With(prometheus.Labels{"operation": "create", "Topic": consumerTuple.C.Topic}).Inc()
				return err
			}
			metricTopicLeasesCounter.With(prometheus.Labels{"operation": "create", "Topic": consumerTuple.C.Topic}).Inc()
		}
	}
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

	err := s.listeners.CreateTimeline(ctx, string(req.Id()), settings)
	builder := flatbuffers.NewBuilder(128)
	builder.Reset()

	if err != nil {
		s.logger.WithError(err).Error("creation failed")
	}
	var derror derrors.Dejaror

	if err != nil {
		switch v := err.(type) {
		case derrors.Dejaror:
			derror = v
		default:
			s.logger.WithError(err).Error("timelineCreate unknown error type returned")
			derror = derrors.NewDejaror("failed creation", "create")
		}
	}

	builder.Finish(writeDejaror(derror, builder))
	return builder, nil
}

func (s *GRPCServer) TimelineCreateMessages(stream grpc.Broker_TimelineCreateMessagesServer) error {
	var request *grpc.TimelineCreateMessageRequest
	var storageRequest timeline.InsertMessagesRequest
	var err error
	var errGet error
	var producer *Producer
	var replyError derrors.Dejaror
	var emptyIDError *derrors.MessageIDTuple

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
				switch v := errGet.(type) {
				case derrors.Dejaror:
					replyError = v
				default:
					replyError = derrors.Dejaror{
						Severity: derrors.SeverityError,
						Message:  v.Error(),
					}
				}
				break
			}
		}

		if len(request.IdBytes()) == 0 {
			if emptyIDError == nil {
				emptyIDError = &derrors.MessageIDTuple{MsgID: nil, MsgError: derrors.Dejaror{Message: "at least one message was missing the ID"}}
			}
			continue
		}

		storageRequest.Messages = append(storageRequest.Messages, timeline.InsertMessagesRequestItem{
			ID:          request.IdBytes(),
			TimestampMS: request.TimeoutMS(),
			Body:        request.BodyBytes(),
			Version:     1,
		})
	}

	if producer == nil {
		replyError = derrors.Dejaror{
			Severity: derrors.SeverityError,
			Message:  "no message with sessionID",
		}
	}

	//returns the response to the client
	var builder *flatbuffers.Builder
	builder = flatbuffers.NewBuilder(128)
	var messagesErrors derrors.MessageIDTupleList

	if replyError.Message == "" {
		if len(storageRequest.Messages) == 0 {
			if emptyIDError == nil {
				//no messages were sent, nothing to do
				return nil
			}

			//we let the client knowns that all its messages were missing the ID
			replyError = emptyIDError.MsgError
		} else {
			storageRequest.ProducerGroupID = producer.GroupID
			storageRequest.TimelineID = producer.Topic

			storageError := s.listeners.CreateMessagesListener(stream.Context(), storageRequest)
			switch v := storageError.(type) {
			case derrors.Dejaror:
				replyError = v
			case derrors.MessageIDTupleList:
				messagesErrors = v
			default:
				replyError = derrors.Dejaror{
					Severity: derrors.SeverityError,
					Message:  v.Error(),
				}
			}
		}
	}

	metricsLabels := prometheus.Labels{"operation": "create"}
	if producer != nil {
		metricsLabels["Topic"] = producer.Topic
	} else {
		metricsLabels["Topic"] = ""
	}
	failedMsgs := len(messagesErrors)
	if failedMsgs == 0 && replyError.Message != "" {
		//we presume all of them failed
		failedMsgs = len(storageRequest.Messages)
	}
	metricMessagesCounterError.With(metricsLabels).Add(float64(len(messagesErrors)))
	metricMessagesCounterSuccess.With(metricsLabels).Add(float64(len(storageRequest.Messages) - failedMsgs))

	root := writeTimelineResponse(replyError, messagesErrors, builder)
	builder.Finish(root)
	err = stream.SendMsg(builder)
	if err != nil {
		s.logger.WithError(err).Error("timeline create msgs failed to send response")
	}

	return nil
}

func (s *GRPCServer) TimelineDelete(stream grpc.Broker_TimelineDeleteServer) error {
	//TODO check the sessionID

	var err error
	var topicErr error
	var sessionID string
	var replyError derrors.Dejaror

	var req *grpc.TimelineDeleteRequest
	var storageRequest timeline.DeleteMessagesRequest

	for {
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		storageRequest.Messages = append(storageRequest.Messages, timeline.DeleteMessagesRequestItem{
			MessageID: req.MessageIDBytes(),
			BucketID:  req.BucketID(),
			Version:   req.Version(),
		})

		if storageRequest.TimelineID == "" {
			storageRequest.TimelineID, topicErr = s.listeners.DeleteRequest(stream.Context(), string(req.SessionID()))

			if topicErr != nil {
				s.logger.WithError(err).Error("delete request from unknown client")
				replyError = derrors.Dejaror{
					Severity: derrors.SeverityError,
					Message:  "handshake required",
				}
				break
			}
		}
		if sessionID == "" {
			sessionID = string(req.SessionID())
		}
	}

	builder := flatbuffers.NewBuilder(128)
	var messagesErrors []derrors.MessageIDTuple

	//timelineID can be nil when no messages arrived
	if storageRequest.TimelineID == "" && replyError.Message != "" {
		replyError = derrors.Dejaror{
			Severity: derrors.SeverityError,
			Message:  "topicID missing and required",
		}
	}
	if sessionID != "" && replyError.Message != "" {
		replyError = derrors.Dejaror{
			Severity: derrors.SeverityError,
			Message:  "sessionID cannot be found",
		}
	}
	if replyError.Message == "" {
		storageError := s.listeners.DeleteMessagesListener(stream.Context(), sessionID, storageRequest)
		if storageError != nil {
			switch v := storageError.(type) {
			case derrors.Dejaror:
				replyError = v
			case derrors.MessageIDTupleList:
				messagesErrors = v
			default:
				replyError = derrors.Dejaror{
					Message: err.Error(),
				}
			}
		}
	}

	metricsLabels := prometheus.Labels{"operation": "delete"}
	if storageRequest.TimelineID != "" {
		metricsLabels["Topic"] = storageRequest.TimelineID
	}
	failedMsgs := len(messagesErrors)
	if failedMsgs == 0 && replyError.Message != "" {
		//we presume all of them failed
		failedMsgs = len(storageRequest.Messages)
	}
	metricMessagesCounterError.With(metricsLabels).Add(float64(len(messagesErrors)))
	metricMessagesCounterSuccess.With(metricsLabels).Add(float64(len(storageRequest.Messages) - failedMsgs))

	rootPosition := writeTimelineResponse(replyError, messagesErrors, builder)
	builder.Finish(rootPosition)
	return stream.SendAndClose(builder)
}

// writes a TimelineResponse message and returns its root
func writeTimelineResponse(mainError derrors.Dejaror, msgIndividualErrors []derrors.MessageIDTuple, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	var rootListErrors *flatbuffers.UOffsetT
	var rootMainErr *flatbuffers.UOffsetT

	if len(msgIndividualErrors) > 0 {
		eachErrorOffset := make([]flatbuffers.UOffsetT, len(msgIndividualErrors))
		for i := range msgIndividualErrors {
			tupleError := writeDejaror(msgIndividualErrors[i].MsgError, builder)
			messageIDRoot := builder.CreateByteVector(msgIndividualErrors[i].MsgID)
			grpc.TimelineMessageIDErrorTupleStart(builder)
			grpc.TimelineMessageIDErrorTupleAddMessgeID(builder, messageIDRoot)
			grpc.TimelineMessageIDErrorTupleAddErr(builder, tupleError)
			eachErrorOffset[i] = grpc.TimelineMessageIDErrorTupleEnd(builder)
		}

		grpc.TimelineMessageIDErrorTupleStartMessgeIDVector(builder, len(msgIndividualErrors))
		for i := range eachErrorOffset {
			builder.PrependUOffsetT(eachErrorOffset[i])
		}
		tmp := builder.EndVector(len(msgIndividualErrors))
		rootListErrors = &tmp
	}

	if mainError.Message != "" {
		tmp := writeDejaror(mainError, builder)
		rootMainErr = &tmp
	}

	grpc.TimelineResponseStart(builder)
	if rootMainErr != nil {
		grpc.TimelineResponseAddErr(builder, *rootMainErr)
	}
	if rootListErrors != nil {
		grpc.TimelineResponseAddMessagesErrors(builder, *rootListErrors)
	}
	return grpc.TimelineResponseEnd(builder)
}

func writeError(err error, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	var derr derrors.Dejaror
	switch v := err.(type) {
	case derrors.Dejaror:
		derr = v
	default:
		derr = derrors.Dejaror{
			Severity: derrors.SeverityError,
			Message:  v.Error(),
		}
	}
	return writeError(derr, builder)
}
func writeDejaror(err derrors.Dejaror, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	//TODO first add the details tuples (map to tuples)

	//no error
	if err.Message == "" {
		grpc.ErrorStart(builder)
		return grpc.ErrorEnd(builder)
	}

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
