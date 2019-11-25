package coordinator

import (
	"context"
	"math/rand"
	"time"
	"unsafe"

	"github.com/bgadrian/dejaq-broker/common/protocol"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	"github.com/bgadrian/dejaq-broker/broker/pkg/synchronization"
	"github.com/bgadrian/dejaq-broker/common"
	"github.com/bgadrian/dejaq-broker/common/errors"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/prometheus/common/log"
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
	HydrateStatus   protocol.HydrationStatus
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
		ConsumerHandshake: c.consumerHandshake,
		ConsumerConnected: func(ctx context.Context, sessionID string) (chan timeline.Lease, error) {
			return c.greeter.ConsumerConnected(sessionID)
		},
		ConsumerDisconnected: c.consumerDisconnected,
		DeleteMessagesListener: func(ctx context.Context, timelineID string, msgs []timeline.Message) []errors.MessageIDTuple {
			//TODO add here a way to identify the consumer or producer
			//only specific clients can delete specific messages
			return c.storage.Delete(ctx, []byte(timelineID), msgs)
		},
		ProducerHandshake: c.producerHandshake,
		CreateTimeline:    c.createTopic,
		CreateMessagesRequest: func(ctx context.Context, sessionID string) (*Producer, error) {
			return c.greeter.GetProducerSessionData(sessionID)
		},
		CreateMessagesListener: c.listenerTimelineCreateMessages,
		DeleteRequest: func(ctx context.Context, sessionID string) (topicID string, err error) {
			topic, err := c.greeter.GetTopicFor(sessionID)
			if err != nil {
				return "", err
			}
			_, err = c.synchronization.GetTopic(ctx, topic)
			if err != nil {
				return "", err
			}
			return topic, nil
		},
	})
}

func (c *Coordinator) consumerHandshake(ctx context.Context, consumer *Consumer) (string, error) {
	_, err := c.synchronization.GetTopic(ctx, consumer.Topic)
	if err != nil {
		return "", err
	}
	sessionID, err := c.greeter.ConsumerHandshake(consumer)
	if err != nil {
		log.Error(err)
		return sessionID, err
	}

	consumerSync := synchronization.Consumer{
		SessionID:        sessionID,
		OverseerBrokerID: consumer.Cluster,
		CarrierBrokerID:  consumer.Cluster,
	}
	consumerSync.ConsumerID = consumer.GetID()
	consumerSync.Topic = string(consumer.GetTopic())
	// TODO add the rest of the params

	err = c.synchronization.AddConsumer(ctx, consumerSync)
	if err != nil {
		log.Error(err)
		return sessionID, err
	}

	return sessionID, err
}

func (c *Coordinator) consumerDisconnected(ctx context.Context, sessionID string) error {
	consumer, err := c.greeter.GetConsumer(sessionID)
	if err != nil {
		log.Error(err)
		return err
	}

	c.greeter.ConsumerDisconnected(sessionID)
	return c.synchronization.RemoveConsumer(ctx, consumer.Topic, string(consumer.ID))
}

func (c *Coordinator) producerHandshake(ctx context.Context, producer *Producer) (string, error) {
	sessionID, err := c.greeter.ProducerHandshake(producer)
	if err != nil {
		log.Error(err)
		return sessionID, err
	}
	producerSync := synchronization.Producer{
		SessionID:        sessionID,
		ProducerID:       producer.ProducerID,
		OverseerBrokerID: producer.Cluster,
		CarrierBrokerID:  producer.Cluster,
	}
	producerSync.Cluster = producer.Cluster
	producerSync.Topic = producer.Topic

	err = c.synchronization.AddProducer(ctx, producerSync)
	if err != nil {
		log.Error(err)
		return sessionID, err
	}
	return sessionID, err
}

func (c *Coordinator) createTopic(ctx context.Context, topic string, settings timeline.TopicSettings) {
	err := c.storage.CreateTopic(ctx, topic)
	if err != nil {
		log.Error(err)
		return
	}
	syncTopic := synchronization.Topic{}
	syncTopic.ID = topic
	syncTopic.CreationTimestamp = dtime.TimeToMS(time.Now())
	syncTopic.ProvisionStatus = protocol.TopicProvisioningStatus_Live
	syncTopic.Settings = settings

	err = c.synchronization.AddTopic(ctx, syncTopic)
	if err != nil {
		log.Error(err)
	}
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, topic string, msgs []timeline.Message) []errors.MessageIDTuple {
	metricMessagesCounter.Inc(int64(len(msgs)))
	for i := range msgs {
		msgs[i].BucketID = uint16(rand.Intn(int(c.conf.NoBuckets)))
	}
	return c.storage.Insert(ctx, []byte(topic), msgs)
}
