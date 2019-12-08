package synchronization

import (
	"context"

	"github.com/dejaq/prototype/client/timeline/consumer"
	"github.com/dejaq/prototype/client/timeline/producer"
	"github.com/dejaq/prototype/common/timeline"
)

type Topic struct {
	timeline.Topic
}

type Consumer struct {
	consumer.Config

	SessionID        string
	OverseerBrokerID string
	CarrierBrokerID  string
}

type Producer struct {
	producer.Config

	SessionID        string
	ProducerID       string
	OverseerBrokerID string
	CarrierBrokerID  string
}

type Catalog interface {
	AddTopic(ctx context.Context, topic Topic) error
	UpdateTopic(ctx context.Context, topic Topic) error
	GetTopic(ctx context.Context, topicID string) (Topic, error)
	RemoveTopic(ctx context.Context, topicID string) error

	AddConsumer(ctx context.Context, consumer Consumer) error
	UpdateConsumer(ctx context.Context, consumer Consumer) error
	GetConsumer(ctx context.Context, topic, consumerID string) (Consumer, error)
	RemoveConsumer(ctx context.Context, topic, consumerID string) error

	AddProducer(ctx context.Context, producer Producer) error
	UpdateProducer(ctx context.Context, producer Producer) error
	GetProducer(ctx context.Context, topic, producerID string) (Producer, error)
	RemoveProducer(ctx context.Context, topic, producerID string) error
}
