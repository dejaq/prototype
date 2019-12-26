package coordinator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"

	"github.com/dejaq/prototype/common/protocol"

	"github.com/dejaq/prototype/broker/domain"
	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	"github.com/dejaq/prototype/broker/pkg/synchronization"
	"github.com/dejaq/prototype/common/errors"
	derrors "github.com/dejaq/prototype/common/errors"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/prometheus/common/log"
	"github.com/rcrowley/go-metrics"
)

const (
	numberOfTopics    = "topics"
	numberOfTimelines = "timelines"
	numberOfMessages  = "messages"
)

var (
	metricTopicsCounter       = metrics.NewRegisteredCounter(numberOfTopics, nil)
	metricTimelinesCounter    = metrics.NewRegisteredCounter(numberOfTimelines, nil)
	metricMessagesCounter     = metrics.NewRegisteredCounter(numberOfMessages, nil)
	ErrConsumerNotConnected   = errors.NewDejaror("consumer not connected", "load")
	ErrUnknownDeleteRequester = errors.NewDejaror("unknown hwo wants to delete messages", "delete")
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
	baseCtx      context.Context
	conf         *Config
	dealer       Dealer
	storage      storage.Repository
	catalog      synchronization.Catalog
	greeter      *Greeter
	loadersMutex sync.Mutex
	loaders      map[string]*Loader
}

type Config struct {
}

func NewCoordinator(ctx context.Context, config *Config, timelineStorage storage.Repository, catalog synchronization.Catalog, greeter *Greeter, dealer Dealer) *Coordinator {
	c := Coordinator{
		baseCtx:      ctx,
		conf:         config,
		storage:      timelineStorage,
		catalog:      catalog,
		greeter:      greeter,
		loadersMutex: sync.Mutex{},
		loaders:      make(map[string]*Loader, 10),
		dealer:       dealer,
	}

	return &c
}

func (c *Coordinator) AttachToServer(server *GRPCServer) {
	server.SetListeners(&TimelineListeners{
		ConsumerHandshake:    c.consumerHandshake,
		ConsumerConnected:    c.consumerConnected,
		ConsumerDisconnected: c.consumerDisconnected,
		DeleteMessagesListener: func(ctx context.Context, sessionID string, timelineID string, messageIDs []timeline.MessageRequestDetails) []errors.MessageIDTuple {
			// check hwo wants to delete message based on sessionID
			var actor []byte
			var ID []byte

			if consumer, err := c.greeter.GetConsumer(sessionID); err == nil {
				actor = []byte("CONSUMER")
				ID = consumer.ID
			} else if producer, err := c.greeter.GetProducerSessionData(sessionID); err == nil {
				actor = []byte("PRODUCER")
				ID = producer.GroupID
			} else {
				var derror derrors.Dejaror
				derror.Module = derrors.ModuleBroker
				derror.Operation = "deleteMessageListener"
				derror.Message = fmt.Sprintf("coordinator not able to find delete requester with sessionID: %s", sessionID)
				derror.ShouldRetry = false
				derror.WrappedErr = ErrUnknownDeleteRequester
				logrus.WithError(derror)

				return []derrors.MessageIDTuple{derrors.MessageIDTuple{Error: derror}}
			}

			return c.storage.Delete(ctx, timeline.DeleteMessages{
				Timestamp:   dtime.TimeToMS(time.Now()),
				DeleterType: actor,
				DeleterID:   ID,
				TimelineID:  []byte(timelineID),
				Messages:    messageIDs,
			})
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
			_, err = c.catalog.GetTopic(ctx, topic)
			if err != nil {
				return "", err
			}
			return topic, nil
		},
	})
}

func (c *Coordinator) getConsumer(ctx context.Context, sessionID string) (*Consumer, error) {
	return c.greeter.GetConsumer(sessionID)
}

func (c *Coordinator) consumerHandshake(ctx context.Context, consumer *Consumer) (string, error) {
	_, err := c.catalog.GetTopic(ctx, consumer.Topic)
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

	err = c.catalog.AddConsumer(ctx, consumerSync)
	if err != nil {
		log.Error(err)
		return sessionID, err
	}

	return sessionID, err
}

func (c *Coordinator) consumerConnected(ctx context.Context, sessionID string) (chan timeline.Lease, error) {
	//create the loader for it
	consumer, err := c.greeter.GetConsumer(sessionID)
	if err != nil {
		return nil, err
	}
	c.loadersMutex.Lock()
	defer c.loadersMutex.Unlock()

	if _, loaderExists := c.loaders[consumer.Topic]; !loaderExists {
		topic, err := c.catalog.GetTopic(ctx, consumer.Topic)
		if err != nil {
			return nil, err
		}
		c.loaders[consumer.Topic] = NewLoader(&LConfig{
			PrefetchMaxNoMsgs:       1000,
			PrefetchMaxMilliseconds: 0,
			Topic:                   &topic.Topic,
		}, c.storage, c.dealer, c.greeter)
		c.loaders[consumer.Topic].Start(c.baseCtx)
		logrus.Infof("started consumer loader for topic: %s", consumer.Topic)
	}
	return c.greeter.ConsumerConnected(sessionID)
}
func (c *Coordinator) consumerDisconnected(ctx context.Context, sessionID string) error {
	consumer, err := c.greeter.GetConsumer(sessionID)
	if err != nil {
		log.Error(err)
		return err
	}

	//TODO if is the last consumer for this topic, stop and delete the Loader c.loaders[consumer.Topic]
	c.greeter.ConsumerDisconnected(sessionID)
	return c.catalog.RemoveConsumer(ctx, consumer.Topic, string(consumer.ID))
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

	err = c.catalog.AddProducer(ctx, producerSync)
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

	err = c.catalog.AddTopic(ctx, syncTopic)
	if err != nil {
		log.Error(err)
	}
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, topicID string, msgs []timeline.Message) []errors.MessageIDTuple {
	metricMessagesCounter.Inc(int64(len(msgs)))
	topic, _ := c.catalog.GetTopic(ctx, topicID)
	for i := range msgs {
		msgs[i].BucketID = uint16(rand.Intn(int(topic.Settings.BucketCount)))
		if msgs[i].GetID() == "" {
			logrus.Fatalf("coordinator received empty messageID")
		}
	}
	return c.storage.Insert(ctx, []byte(topicID), msgs)
}
