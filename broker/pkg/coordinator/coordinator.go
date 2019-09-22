package coordinator

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/common"
	"github.com/bgadrian/dejaq-broker/common/errors"

	"github.com/bgadrian/dejaq-broker/common/timeline"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
)

var (
	defaultTimelineID = "default_timeline"
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
	storage   storage.Repository
	ticker    *time.Ticker
	buckets   []uint16
	consumers []*Consumer
	server    *GRPCServer
	lock      *sync.RWMutex
}

type CoordinatorConfig struct {
	TopicType    common.TopicType
	NoBuckets    int
	TickInterval time.Duration
}

func NewCoordinator(ctx context.Context, config CoordinatorConfig, timelineStorage storage.Repository, server *GRPCServer) *Coordinator {
	c := Coordinator{
		storage:   timelineStorage,
		ticker:    time.NewTicker(config.TickInterval),
		server:    server,
		consumers: []*Consumer{},
		lock:      &sync.RWMutex{},
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
	switch topicType {
	case common.TopicType_Timeline:
		c.buckets = make([]uint16, noBuckets)
		for i := range c.buckets {
			c.buckets[i] = uint16(rand.Intn(len(c.buckets)))
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
	available, _, _ := c.storage.Select(ctx, GetDefaultTimelineID(), consumer.AssignedBuckets, 10, uint64(time.Now().UTC().Unix()))
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
	}
	err := c.server.pushMessagesToConsumer(ctx, consumer.ID, toSend)
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

	var consumerIndex int
	for bi := range c.buckets {
		c.consumers[consumerIndex].AssignedBuckets = append(c.consumers[consumerIndex].AssignedBuckets, uint16(bi))
		consumerIndex++
		if consumerIndex >= len(c.consumers) {
			consumerIndex = 0
		}
	}
	c.lock.Unlock()
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, msgs []timeline.Message) []errors.MessageIDTuple {
	for _, msg := range msgs {
		msg.BucketID = c.buckets[rand.Intn(len(c.buckets))]
	}
	return c.storage.Insert(ctx, GetDefaultTimelineID(), msgs)
}
