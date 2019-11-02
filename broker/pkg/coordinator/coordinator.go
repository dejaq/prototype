package coordinator

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
	"unsafe"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	"github.com/rcrowley/go-metrics"

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
	metricTopicsCounter    = metrics.NewRegisteredCounter(numberOfTopics, nil)
	metricTimelinesCounter = metrics.NewRegisteredCounter(numberOfTimelines, nil)
	metricMessagesCounter  = metrics.NewRegisteredCounter(numberOfMessages, nil)
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

type Producer struct {
	GroupID []byte
	Topic   string
	Cluster string
}

type Coordinator struct {
	conf            *Config
	dealer          Dealer
	storage         storage.Repository
	synchronization synchronization.Repository
	ticker          *time.Ticker
	consumers       []*Consumer
	lock            *sync.RWMutex
	greeter         *Greeter
}

type Config struct {
	TopicType    common.TopicType
	NoBuckets    uint16
	TickInterval time.Duration
}

func NewCoordinator(ctx context.Context, config *Config, timelineStorage storage.Repository, synchronization synchronization.Repository, greeter *Greeter) *Coordinator {
	c := Coordinator{
		conf:            config,
		storage:         timelineStorage,
		synchronization: synchronization,
		ticker:          time.NewTicker(config.TickInterval),
		consumers:       []*Consumer{},
		lock:            &sync.RWMutex{},
		greeter:         greeter,
	}

	c.dealer = NewExclusiveDealer()

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

func (c *Coordinator) AttachToServer(server *GRPCServer) {
	server.SetListeners(&GRPCListeners{
		TimelineCreateMessagesListener: c.listenerTimelineCreateMessages,
		TimelineConsumerSubscribed: func(ctx context.Context, consumer Consumer) {
			c.RegisterCustomer(consumer)
		},
		TimelineConsumerUnSubscribed: func(ctx context.Context, consumer Consumer) {
			c.DeRegisterCustomer(consumer)
		},
		TimelineDeleteMessagesListener: func(ctx context.Context, timelineID []byte, msgs []timeline.Message) []errors.MessageIDTuple {
			//TODO add here a way to identify the consumer or producer
			//only specific clients can delete specific messages
			return c.storage.Delete(ctx, timelineID, msgs)
		},
		TimelineProducerSubscribed: func(i context.Context, producer Producer) {

		},
	})
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

	consumerPipeline, err := c.greeter.GetPipelineFor(consumer.GetID())
	if err != nil {
		//TODO put a timeout and unsubscribe it?
		//TODO cancel the leases
		log.Println("loadCustomerMessages failed", err)
		return
	}
	//TODO add timeout here, as each message reaches to client and can take a while
	//select{
	//case time.After(3 * time.Second):
	//	return errors.New("pushing messages timeout")
	//
	//}
	for i := range toSend {
		consumerPipeline <- toSend[i]
	}
}

func (c *Coordinator) RegisterCustomer(consumer Consumer) {
	c.lock.Lock()

	// TODO validate it didn't used another registered consumer's id
	c.consumers = append(c.consumers, &consumer)

	c.dealer.Shuffle(c.consumers, c.conf.NoBuckets)

	c.lock.Unlock()
}

func (c *Coordinator) DeRegisterCustomer(consumer Consumer) {
	c.lock.Lock()

	for i, cons := range c.consumers {
		if string(cons.ID) == string(consumer.ID) {
			c.consumers = append(c.consumers[:i], c.consumers[i+1:]...)
			break
		}
	}

	c.dealer.Shuffle(c.consumers, c.conf.NoBuckets)

	c.lock.Unlock()
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, topic []byte, msgs []timeline.Message) []errors.MessageIDTuple {
	metricMessagesCounter.Inc(int64(len(msgs)))
	for i := range msgs {
		msgs[i].BucketID = uint16(rand.Intn(int(c.conf.NoBuckets)))
	}
	return c.storage.Insert(ctx, topic, msgs)
}
