package producer

import (
	"context"
	"github.com/dejaq/prototype/common/metrics/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"sync"

	derror "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
	dejaq "github.com/dejaq/prototype/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	metricTopicMessagesCounter = exporter.GetProducerCounter("topic_messages_count", []string{"operation", "topic"})
	metricTopicMessagesErrors = exporter.GetProducerCounter("topic_messages_errors", []string{"operation", "topic"})
)

type Config struct {
	Cluster         string
	Topic           string
	ProducerGroupID string
	ProducerID      string
}

type Producer struct {
	conf           *Config
	overseer       dejaq.BrokerClient
	carrier        dejaq.BrokerClient
	sessionID      string
	handshakeMutex sync.RWMutex
}

// NewProducer creates a new timeline producer
func NewProducer(overseer dejaq.BrokerClient, carrier *grpc.ClientConn, conf *Config) *Producer {
	result := &Producer{
		conf:     conf,
		overseer: overseer,
		carrier:  dejaq.NewBrokerClient(carrier),
	}
	return result
}

// Handshake has to be called before inserting or when Inserting
// fails with an invalid/expired session
func (c *Producer) Handshake(ctx context.Context) error {
	c.handshakeMutex.Lock()
	defer c.handshakeMutex.Unlock()

	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)
	clusterPos := builder.CreateString(c.conf.Cluster)
	producerGroupPos := builder.CreateString(c.conf.ProducerGroupID)
	topicIDPos := builder.CreateString(c.conf.Topic)
	producerIDPos := builder.CreateString(c.conf.ProducerID)
	dejaq.TimelineProducerHandshakeRequestStart(builder)
	dejaq.TimelineProducerHandshakeRequestAddCluster(builder, clusterPos)
	dejaq.TimelineProducerHandshakeRequestAddProducerGroupID(builder, producerGroupPos)
	dejaq.TimelineProducerHandshakeRequestAddTopicID(builder, topicIDPos)
	dejaq.TimelineProducerHandshakeRequestAddProducerID(builder, producerIDPos)
	root := dejaq.TimelineProducerHandshakeRequestEnd(builder)
	builder.Finish(root)

	resp, err := c.overseer.TimelineProducerHandshake(ctx, builder)
	if err != nil {
		return err
	}
	c.sessionID = string(resp.SessionID())
	return nil
}

// InsertMessages creates a stream and push all the messages.
//It fails if it does not have a valid session from the overseer
//Thread safe
// Returns a simple error, Dejaror or MessageIDTupleList
func (c *Producer) InsertMessages(ctx context.Context, msgs []timeline.Message) error {
	c.handshakeMutex.RLock()
	defer c.handshakeMutex.RLock()

	if c.sessionID == "" {
		return errors.New("missing sessionID")
	}

	stream, err := c.carrier.TimelineCreateMessages(ctx)
	if err != nil {
		return err
	}
	var builder *flatbuffers.Builder

	builder = flatbuffers.NewBuilder(128)
	var root flatbuffers.UOffsetT

	for i := range msgs {
		builder.Reset()

		idPosition := builder.CreateByteVector(msgs[i].ID)
		bodyPosition := builder.CreateByteVector(msgs[i].Body)
		sessionIDPosition := builder.CreateString(c.sessionID)
		dejaq.TimelineCreateMessageRequestStart(builder)
		dejaq.TimelineCreateMessageRequestAddId(builder, idPosition)
		dejaq.TimelineCreateMessageRequestAddSessionID(builder, sessionIDPosition)
		dejaq.TimelineCreateMessageRequestAddTimeoutMS(builder, msgs[i].TimestampMS)
		dejaq.TimelineCreateMessageRequestAddBody(builder, bodyPosition)
		root = dejaq.TimelineCreateMessageRequestEnd(builder)

		builder.Finish(root)
		err = stream.Send(builder)
		//TODO if err is invalid/expired sessionID do a handshake automatically
		if err != nil {
			metricTopicMessagesErrors.With(prometheus.Labels{"operation":"insert", "topic":c.GetTopic()}).Inc()
			return errors.Wrap(err, "failed stream")
		}
		metricTopicMessagesCounter.With(prometheus.Labels{"operation":"insert", "topic":c.GetTopic()}).Inc()
	}

	response, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		metricTopicMessagesErrors.With(prometheus.Labels{"operation":"insert", "topic":c.GetTopic()}).Inc()
		return errors.Wrap(err, "failed to send the events")
	}
	err = derror.ParseTimelineResponse(response)
	if err != nil {
		metricTopicMessagesErrors.With(prometheus.Labels{"operation":"insert", "topic":c.GetTopic()}).Inc()
		return err
	}
	return nil
}

func (c *Producer) GetProducerGroupID() string {
	return c.conf.ProducerGroupID
}
func (c *Producer) GetProducerID() string {
	return c.conf.ProducerID
}

func (c *Producer) GetTopic() string {
	return c.conf.Topic
}
