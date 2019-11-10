package coordinator

import (
	"context"
	"math/rand"
	"sync"
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
	GroupID []byte
	Topic   string
	Cluster string
}

type Coordinator struct {
	conf            *Config
	dealer          Dealer
	storage         storage.Repository
	synchronization synchronization.Repository
	lock            *sync.RWMutex
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
		lock:            &sync.RWMutex{},
		greeter:         greeter,
		loader:          loader,
		dealer:          dealer,
	}

	c.loader.Start(ctx)

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
		TimelineDeleteMessagesListener: func(ctx context.Context, timelineID string, msgs []timeline.Message) []errors.MessageIDTuple {
			//TODO add here a way to identify the consumer or producer
			//only specific clients can delete specific messages
			return c.storage.Delete(ctx, []byte(timelineID), msgs)
		},
		TimelineProducerSubscribed: func(i context.Context, producer Producer) {

		},
	})
}

func (c *Coordinator) RegisterCustomer(consumer Consumer) {
	c.lock.Lock()
	defer c.lock.Unlock()
}

func (c *Coordinator) DeRegisterCustomer(consumer Consumer) {
	c.lock.Lock()
	defer c.lock.Unlock()
}

func (c *Coordinator) listenerTimelineCreateMessages(ctx context.Context, topic string, msgs []timeline.Message) []errors.MessageIDTuple {
	metricMessagesCounter.Inc(int64(len(msgs)))
	for i := range msgs {
		msgs[i].BucketID = uint16(rand.Intn(int(c.conf.NoBuckets)))
	}
	return c.storage.Insert(ctx, []byte(topic), msgs)
}
