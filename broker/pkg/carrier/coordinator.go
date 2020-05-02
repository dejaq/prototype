package carrier

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/dejaq/prototype/common/metrics/exporter"
	"github.com/prometheus/client_golang/prometheus"

	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"
	"github.com/dejaq/prototype/broker/pkg/synchronization"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/protocol"
	dtime "github.com/dejaq/prototype/common/time"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrUnknownDeleteRequester = derrors.Dejaror{
		Severity:  derrors.SeverityError,
		Message:   "unknown client, handshake required",
		Module:    derrors.ModuleBroker,
		Operation: "deleteMessage",
	}
)

type Producer struct {
	GroupID    string
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
	logger       logrus.FieldLogger
}

type Config struct {
	LoaderMaxBatchSize       int
	AutoCreateTopicsSettings *timeline.TopicSettings
}

func NewCoordinator(ctx context.Context, config *Config, timelineStorage storage.Repository, catalog synchronization.Catalog, greeter *Greeter, dealer Dealer, logger logrus.FieldLogger) *Coordinator {
	c := Coordinator{
		baseCtx:      ctx,
		conf:         config,
		storage:      timelineStorage,
		catalog:      catalog,
		greeter:      greeter,
		loadersMutex: sync.Mutex{},
		loaders:      make(map[string]*Loader, 10),
		dealer:       dealer,
		logger:       logger,
	}

	go c.watchStorage()

	return &c
}

func (c *Coordinator) AttachToServer(server *GRPCServer) {
	server.SetListeners(&TimelineListeners{
		ConsumerHandshake:      c.consumerHandshake,
		ConsumerConnected:      c.consumerConnected,
		ConsumerDisconnected:   c.consumerDisconnected,
		ConsumerStatus:         c.consumerStatus,
		DeleteMessagesListener: c.listenerTimelineDeleteMessages,
		ProducerHandshake:      c.producerHandshake,
		CreateTimeline:         c.createTopic,
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

func (c *Coordinator) consumerHandshake(ctx context.Context, consumer *Consumer) (string, error) {
	_, err := c.catalog.GetTopic(ctx, consumer.GetTopic())
	if err != nil {
		if err == derrors.ErrTopicNotFound && c.conf.AutoCreateTopicsSettings != nil {
			err = c.createTopic(ctx, consumer.Topic, *c.conf.AutoCreateTopicsSettings)
			if err != nil {
				return "", errors.Wrap(err, "auto create Topic failed")
			}
			//else carry on with the logic
			c.logger.Infof("auto created Topic %s based on consumer handshake", consumer.Topic)
		} else {
			return "", err
		}
	}
	sessionID, err := c.greeter.ConsumerHandshake(consumer)
	if err != nil {
		return sessionID, err
	}

	consumerSync := synchronization.Consumer{
		SessionID:        sessionID,
		OverseerBrokerID: consumer.GetCluster(),
		CarrierBrokerID:  consumer.GetCluster(),
	}
	consumerSync.ConsumerID = consumer.GetID()
	consumerSync.Topic = consumer.GetTopic()
	// TODO add the rest of the params

	err = c.catalog.AddConsumer(ctx, consumerSync)
	if err != nil {
		return sessionID, err
	}

	return sessionID, err
}

func (c *Coordinator) consumerConnected(ctx context.Context, sessionID string) (*ConsumerPipelineTuple, error) {
	//create the loader for it
	consumer, err := c.greeter.GetConsumer(sessionID)
	if err != nil {
		return nil, err
	}
	c.loadersMutex.Lock()
	defer c.loadersMutex.Unlock()

	if _, loaderExists := c.loaders[consumer.GetTopic()]; !loaderExists {
		topic, err := c.catalog.GetTopic(ctx, consumer.GetTopic())
		if err != nil {
			return nil, err
		}
		c.loaders[consumer.GetTopic()] = NewLoader(&LoaderConfig{
			PrefetchMaxNoMsgs:       1000,
			PrefetchMaxMilliseconds: 0,
			Topic:                   &topic.Topic,
			Timers: LoaderTimerConfig{
				Min:  time.Millisecond * 5,
				Max:  time.Millisecond * 200,
				Step: time.Millisecond * 25,
			},
			MaxBatchSize: c.conf.LoaderMaxBatchSize,
		}, c.storage, c.dealer, c.greeter)
		c.loaders[consumer.GetTopic()].Start(c.baseCtx)
		logrus.Infof("started consumer loader for Topic: %s", consumer.GetTopic())
	}
	return c.greeter.ConsumerConnected(sessionID)
}

func (c *Coordinator) consumerDisconnected(ctx context.Context, sessionID string) error {
	consumer, err := c.greeter.GetConsumer(sessionID)
	if err != nil {
		return err
	}

	//TODO if is the last consumer for this Topic, stop and delete the Loader c.loaders[consumer.Topic]
	c.greeter.ConsumerDisconnected(sessionID)
	return c.catalog.RemoveConsumer(ctx, consumer.GetTopic(), consumer.GetID())
}

func (c *Coordinator) consumerStatus(ctx context.Context, status protocol.ConsumerStatus) error {
	consumer, err := c.greeter.GetConsumer(string(status.SessionID))
	if err != nil {
		return err
	}
	consumer.SetStatus(status)
	return nil
}

func (c *Coordinator) producerHandshake(ctx context.Context, producer *Producer) (string, error) {
	sessionID, err := c.greeter.ProducerHandshake(producer)
	if err != nil {
		if err == derrors.ErrTopicNotFound && c.conf.AutoCreateTopicsSettings != nil {
			err = c.createTopic(ctx, producer.Topic, *c.conf.AutoCreateTopicsSettings)
			if err != nil {
				return "", errors.Wrap(err, "auto create Topic failed")
			}
			c.logger.Infof("auto created Topic %s based on producer handshake", producer.Topic)
			//else carry on with the logic
		} else {
			return "", err
		}
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
		return sessionID, err
	}
	return sessionID, err
}

func (c *Coordinator) createTopic(ctx context.Context, topic string, settings timeline.TopicSettings) error {
	if _, err := c.catalog.GetTopic(ctx, topic); err == nil {
		//already exists
		return nil
	}
	syncTopic := synchronization.Topic{}
	syncTopic.ID = topic
	syncTopic.CreationTimestamp = dtime.TimeToMS(time.Now())
	syncTopic.ProvisionStatus = protocol.TopicProvisioningStatus_Live
	syncTopic.Settings = settings

	err := c.catalog.AddTopic(ctx, syncTopic)
	if err != nil {
		return errors.Wrap(err, "catalog failed")
	}

	err = c.storage.CreateTopic(ctx, topic)
	if err != nil {
		return errors.Wrap(err, "storage failed")
	}
	return nil
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, req timeline.InsertMessagesRequest) error {
	topic, err := c.catalog.GetTopic(ctx, req.GetTimelineID())
	if err != nil {
		return err
	}
	for i := range req.Messages {
		req.Messages[i].BucketID = uint16(rand.Intn(int(topic.Settings.BucketCount)))
	}
	err = c.storage.Insert(ctx, req)
	if err != nil {
	}
	return err
}

func (c *Coordinator) listenerTimelineDeleteMessages(ctx context.Context, sessionID string, req timeline.DeleteMessagesRequest) error {
	// check hwo wants to delete message based on sessionID
	if consumer, err := c.greeter.GetConsumer(sessionID); err == nil {
		req.CallerType = timeline.DeleteCallerConsumer
		req.CallerID = consumer.GetID()
	} else if producer, err := c.greeter.GetProducerSessionData(sessionID); err == nil {
		req.CallerType = timeline.DeleteCallerProducer
		req.CallerID = producer.GroupID
	} else {
		return ErrUnknownDeleteRequester
	}

	err := c.storage.Delete(ctx, req)
	return err
}

//TODO move this to its own component
func (c *Coordinator) watchStorage() {
	t := time.NewTicker(time.Second)

	metricTopicCountByStatus := exporter.GetBrokerCounter("topic_messages_count_by_status", []string{"status", "Topic"})

	for range t.C {
		//this is the consumers lag basically
		topics, err := c.catalog.GetAllTopics(c.baseCtx)
		if err != nil {
			c.logger.WithError(err).Error("failed fetching all topics")
			continue
		}
		for _, t := range topics {
			count, err := c.storage.CountByStatus(c.baseCtx, timeline.CountRequest{Type: timeline.StatusAvailable, TimelineID: t.ID})
			if err != nil {
				c.logger.WithError(err).Error("failed c.storage.CountByStatus")
				continue
			}
			metricTopicCountByStatus.With(prometheus.Labels{"status": "available", "Topic": t.ID}).Add(float64(count))
		}
	}
}
