package coordinator

import (
	"context"
	"github.com/rcrowley/go-metrics"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/broker/pkg/synchronization"

	"github.com/bgadrian/dejaq-broker/common"
	"github.com/bgadrian/dejaq-broker/common/errors"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
)

const (
	numberOfTopics    = "topics"
	numberOfTimelines = "timelines"
	numberOfMessages  = "messages"
)

var (
	defaultTimelineID      = "default_timeline"
	metricTopicsCounter    = metrics.NewRegisteredCounter(numberOfTopics, nil)
	metricTimelinesCounter = metrics.NewRegisteredCounter(numberOfTimelines, nil)
	metricMessagesCounter  = metrics.NewRegisteredCounter(numberOfMessages, nil)
)

func GetDefaultTimelineID() []byte {
	return []byte(defaultTimelineID)
}

type Consumer struct {
	ID              []byte
	AssignedBuckets []uint16
	Topic           string
	Cluster         string
	LeaseMs         uint64
}

type Coordinator struct {
	storage         storage.Repository
	synchronization synchronization.Repository
	ticker          *time.Ticker
	buckets         []uint16
	consumers       []*Consumer
	server          *GRPCServer
	lock            *sync.RWMutex
}

type Config struct {
	TopicType    common.TopicType
	NoBuckets    int
	TickInterval time.Duration
}

func NewCoordinator(ctx context.Context, config Config, timelineStorage storage.Repository, server *GRPCServer, synchronization synchronization.Repository) *Coordinator {
	c := Coordinator{
		storage:         timelineStorage,
		synchronization: synchronization,
		ticker:          time.NewTicker(config.TickInterval),
		server:          server,
		consumers:       []*Consumer{},
		lock:            &sync.RWMutex{},
	}

	c.setupTopic(config.TopicType, defaultTimelineID, config.NoBuckets)

	server.InnerServer.listeners = &GRPCListeners{
		TimelineCreateMessagesListener: c.listenerTimelineCreateMessages,
		TimelineConsumerSubscribed: func(i context.Context, consumer Consumer) {
			c.RegisterCustomer(consumer.ID)
		},
		TimelineConsumerUnSubscribed: func(i context.Context, consumer Consumer) {
			//TODO make unsubscribe
		},
		TimelineDeleteMessagesListener: func(ctx context.Context, msgs []timeline.Message) []errors.MessageIDTuple {
			//TODO add here a way to identify the consumer or producer
			//only specific clients can delete specific messages
			return c.storage.Delete(ctx, GetDefaultTimelineID(), msgs)
		},
	}

	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.loadMessages(ctx)

			case <-ctx.Done():
				c.ticker.Stop()
				return
			}
		}
	}()

	return &c
}

func (c *Coordinator) setupTopic(topicType common.TopicType, topicID string, noBuckets int) {
	metricTopicsCounter.Inc(1)
	switch topicType {
	case common.TopicType_Timeline:
		metricTimelinesCounter.Inc(1)
		c.buckets = make([]uint16, noBuckets)
		for i := range c.buckets {
			c.buckets[i] = uint16(rand.Intn(math.MaxUint16))
		}
	default:
		log.Fatal("not implemented")
	}
}

func (c *Coordinator) loadMessages(ctx context.Context) {
	c.lock.RLock()

	for _, consumer := range c.consumers {
		c.loadCustomerMessages(ctx, consumer)
	}

	c.lock.RUnlock()
}

func (c *Coordinator) loadCustomerMessages(ctx context.Context, consumer *Consumer) {
	available, _, _ := c.storage.Select(ctx, []byte(consumer.Topic), consumer.AssignedBuckets, 10, uint64(time.Now().UTC().Unix()))
	if len(available) == 0 {
		return
	}
	toSend := make([]timeline.PushLeases, len(available))
	for i := range available {
		toSend[i] = timeline.PushLeases{
			ExpirationTimestampMS: uint64(time.Now().UTC().Unix()) + 120,
			ConsumerID:            consumer.ID,
			Message:               timeline.NewLeaseMessage(available[i]),
		}
		available[i].LockConsumerID = consumer.ID
		available[i].TimestampMS += consumer.LeaseMs
	}

	c.storage.UpdateLeases(ctx, []byte(consumer.Topic), available)
	err := c.server.PushLeasesToConsumer(ctx, consumer.ID, toSend)
	if err != nil {
		//cancel the leases!
	}
}

func (c *Coordinator) RegisterCustomer(consumerID []byte) {
	c.lock.Lock()

	c.consumers = append(c.consumers, &Consumer{ID: consumerID})

	for _, consumers := range c.consumers {
		consumers.AssignedBuckets = []uint16{}
	}

	maxBuckets := len(c.buckets)/len(c.consumers) + 1
	for i, b := range c.buckets {
		consumerIndex := i / maxBuckets
		c.consumers[consumerIndex].AssignedBuckets = append(c.consumers[consumerIndex].AssignedBuckets, b)
	}

	c.lock.Unlock()
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, msgs []timeline.Message) []errors.MessageIDTuple {
	metricMessagesCounter.Inc(int64(len(msgs)))
	for i := range msgs {
		msgs[i].BucketID = c.buckets[rand.Intn(len(c.buckets))]
	}
	return c.storage.Insert(ctx, GetDefaultTimelineID(), msgs)
}
