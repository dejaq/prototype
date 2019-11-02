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
	TimelineCreateMessagesListener func(context.Context, []byte, []timeline.Message) []derrors.MessageIDTuple
	TimelineProducerSubscribed     func(context.Context, Producer)
	TimelineConsumerSubscribed     func(context.Context, Consumer)
	TimelineConsumerUnSubscribed   func(context.Context, Consumer)
	TimelineDeleteMessagesListener func(ctx context.Context, msgs []timeline.Message) []derrors.MessageIDTuple
}

// GRPCServer intercept gRPC and sends messages, and transforms to our business logic
type GRPCServer struct {
	InnerServer *innerServer
}

func NewGRPCServer(listeners *GRPCListeners) *GRPCServer {
	s := GRPCServer{

		InnerServer: &innerServer{
			listeners: listeners,
			greeter: &Greeter{
				RWMutex:                       sync.RWMutex{},
				consumerIDsAndSessionIDs:      make(map[string]string, 128),
				consumerSessionIDAndID:        make(map[string]string, 128),
				consumerIDsAndPipelines:       make(map[string]chan timeline.PushLeases), //not buffered!!!
				producerSessionIDs:            make(map[string]*grpc.TimelineProducerHandshakeRequest),
				producerGroupIDsAndSessionIDs: make(map[string]string),
			},
		},
	}
	return &s
}

// TimelineCreateMessagesPusher is used by the coordinator to push msgs when needed
func (s *GRPCServer) PushLeasesToConsumer(ctx context.Context, consumerID []byte, leases []timeline.PushLeases) error {
	s.InnerServer.greeter.RWMutex.RLock()
	defer s.InnerServer.greeter.RWMutex.RUnlock()

	if _, hasSession := s.InnerServer.greeter.consumerIDsAndSessionIDs[string(consumerID)]; !hasSession {
		return errors.New("consumer is not subscribed")
	}

	buffer, isConnectedNow := s.InnerServer.greeter.consumerIDsAndPipelines[string(consumerID)]
	if !isConnectedNow {
		s.InnerServer.listeners.TimelineConsumerUnSubscribed(ctx, Consumer{
			ID: consumerID,
		})
		return errors.New("cannot find the channel connection")
	}

	//TODO add timeout here, as each message reaches to client and can take a while
	//select{
	//case time.After(3 * time.Second):
	//	return errors.New("pushing messages timeout")
	//
	//}
	for i := range leases {
		buffer <- leases[i]
	}
	return nil
}

type innerServer struct {
	listeners *GRPCListeners
	greeter   *Greeter
}

func (s *innerServer) TimelineProducerHandshake(ctx context.Context, req *grpc.TimelineProducerHandshakeRequest) (*flatbuffers.Builder, error) {
	sessionID, err := s.greeter.AddProducer(req)
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

func (s *innerServer) TimelineConsumerHandshake(ctx context.Context, req *grpc.TimelineConsumerHandshakeRequest) (*flatbuffers.Builder, error) {
	sessionID, err := s.greeter.AddConsumer(req)
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

func (s *innerServer) TimelineConsume(req *grpc.TimelineConsumeRequest, stream grpc.Broker_TimelineConsumeServer) error {
	buffer, err := s.greeter.consumerStarted(string(req.SessionID()))
	if err != nil {
		return err
	}

	defer func() {
		s.greeter.consumerStopped(string(req.SessionID()))
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

func (s *innerServer) TimelineCreateMessages(stream grpc.Broker_TimelineCreateMessagesServer) error {
	var msgs []timeline.Message
	var request *grpc.TimelineCreateMessageRequest
	var err error
	var sessionID string
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
			fmt.Errorf("TimelineCreateMessages client failed err=%s", err.Error())
			break
		}

		if sessionData == nil {
			sessionID = string(request.SessionID())
			s.greeter.RLock()
			var hadHandshake bool
			sessionData, hadHandshake = s.greeter.producerSessionIDs[sessionID]
			s.greeter.RUnlock()

			if !hadHandshake {
				return errors.New("producer missing session")
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
		fmt.Errorf("TimelineCreateMessages err=%s", err.Error())
	}

	return nil
}
func (s *innerServer) TimelineExtendLease(grpc.Broker_TimelineExtendLeaseServer) error {
	return nil
}
func (s *innerServer) TimelineRelease(grpc.Broker_TimelineReleaseServer) error {
	return nil
}
func (s *innerServer) TimelineDelete(stream grpc.Broker_TimelineDeleteServer) error {
	//TODO check the sessionID
	var err error
	var req *grpc.TimelineDeleteRequest

	var batch []timeline.Message
	for err == nil {
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		batch = append(batch, timeline.Message{
			ID:              req.MessageIDBytes(),
			TimestampMS:     0,
			BodyID:          nil,
			Body:            nil,
			ProducerGroupID: nil,
			LockConsumerID:  nil,
			BucketID:        req.BucketID(),
			Version:         req.Version(),
		})
	}
	responseErrors := s.listeners.TimelineDeleteMessagesListener(context.Background(), batch)
	builder := flatbuffers.NewBuilder(128)
	rootPosition := writeTimelineResponse(responseErrors, builder)
	builder.Finish(rootPosition)
	return stream.SendAndClose(builder)
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
