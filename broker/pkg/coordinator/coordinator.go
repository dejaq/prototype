package coordinator

import (
	"context"
	"math/rand"
	"unsafe"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	"github.com/bgadrian/dejaq-broker/broker/pkg/synchronization"
	"github.com/bgadrian/dejaq-broker/common"
	"github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/rcrowley/go-metrics"
)

const (
	numberOfTopics    = "topics"
	numberOfTimelines = "timelines"
	numberOfMessages  = "messages"
)

var (
	metricTopicsCounter     = metrics.NewRegisteredCounter(numberOfTopics, nil)
	metricTimelinesCounter  = metrics.NewRegisteredCounter(numberOfTimelines, nil)
	metricMessagesCounter   = metrics.NewRegisteredCounter(numberOfMessages, nil)
	ErrConsumerNotConnected = errors.NewDejaror("consumer not connected", "load")
)

type Consumer struct {
	ID              []byte
	AssignedBuckets []domain.BucketRange
	Topic           string
	Cluster         string
	LeaseMs         uint64
}

func (c Consumer) GetID() string {
	return *(*string)(unsafe.Pointer(&c.ID))
}

// GetTopic ...
func (c Consumer) GetTopic() []byte {
	return []byte(c.Topic)
}

type Producer struct {
	GroupID    []byte
	Topic      string
	Cluster    string
	ProducerID string
}

type Coordinator struct {
	conf            *Config
	dealer          Dealer
	storage         storage.Repository
	synchronization synchronization.Repository
	greeter         *Greeter
	loader          *Loader
}

type Config struct {
	TopicType common.TopicType
	NoBuckets uint16
}

func NewCoordinator(ctx context.Context, config *Config, timelineStorage storage.Repository, synchronization synchronization.Repository, greeter *Greeter, loader *Loader, dealer Dealer) *Coordinator {
	c := Coordinator{
		conf:            config,
		storage:         timelineStorage,
		synchronization: synchronization,
		greeter:         greeter,
		loader:          loader,
		dealer:          dealer,
	}

	c.loader.Start(ctx)

	return &c
}

func (c *Coordinator) AttachToServer(server *GRPCServer) {
	server.SetListeners(&TimelineListeners{
		ConsumerHandshake: func(ctx context.Context, consumer *Consumer) (string, error) {
			//TODO ask the Catalog if the topic exists
			return c.greeter.ConsumerHandshake(consumer)
		},
		ConsumerConnected: func(ctx context.Context, sessionID string) (chan timeline.PushLeases, error) {
			return c.greeter.ConsumerConnected(sessionID)
		},
		ConsumerDisconnected: func(sessionID string) {
			c.greeter.ConsumerDisconnected(sessionID)
		},
		DeleteMessagesListener: func(ctx context.Context, timelineID string, msgs []timeline.Message) []errors.MessageIDTuple {
			//TODO add here a way to identify the consumer or producer
			//only specific clients can delete specific messages
			return c.storage.Delete(ctx, []byte(timelineID), msgs)
		},

		ProducerHandshake: func(i context.Context, producer *Producer) (string, error) {
			//TODO ask the Catalog if the topic exists
			return c.greeter.ProducerHandshake(producer)
		},
		CreateMessagesRequest: func(ctx context.Context, sessionID string) (*Producer, error) {
			return c.greeter.GetProducerSessionData(sessionID)
		},
		CreateMessagesListener: c.listenerTimelineCreateMessages,
		DeleteRequest: func(ctx context.Context, sessionID string) (topicID string, err error) {
			//TODO ask the Catalog if the topic exists
			return c.greeter.GetTopicFor(sessionID)
		},
	})
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, topic string, msgs []timeline.Message) []errors.MessageIDTuple {
	metricMessagesCounter.Inc(int64(len(msgs)))
	for i := range msgs {
		msgs[i].BucketID = uint16(rand.Intn(int(c.conf.NoBuckets)))
	}
	return c.storage.Insert(ctx, []byte(topic), msgs)
}
