package producer

import (
	"context"
	"io"
	"log"
	"sync"

	"github.com/bgadrian/dejaq-broker/common/timeline"
	dejaq "github.com/bgadrian/dejaq-broker/grpc/DejaQ"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"
)

type Config struct {
	Cluster         string
	Topic           string
	ProducerGroupID string
}

type Producer struct {
	conf           *Config
	overseer       dejaq.BrokerClient
	carrier        dejaq.BrokerClient
	sessionID      string
	handshakeMutex sync.RWMutex
	id             string
}

// NewProducer creates a new timeline producer
func NewProducer(overseer, carrier *grpc.ClientConn, conf *Config, producerID string) *Producer {
	result := &Producer{
		conf:     conf,
		overseer: dejaq.NewBrokerClient(overseer),
		carrier:  dejaq.NewBrokerClient(carrier),
		id:       producerID,
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
	producerIDPos := builder.CreateString(c.id)
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
func (c *Producer) InsertMessages(ctx context.Context, msgs []timeline.Message) error {
	c.handshakeMutex.RLock()
	defer c.handshakeMutex.RLock()

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
			log.Fatalf("insert2 err: %s", err.Error())
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		log.Fatalf("insert3 err: %s", err.Error())
	}
	return err
}
