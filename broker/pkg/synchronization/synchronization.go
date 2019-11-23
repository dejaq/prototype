package synchronization

import (
	"context"
	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

type Topic struct {
	timeline.Topic
}

type Consumer struct {
	consumer.Config

	OverseerBrokerID string
	CarrierBrokerID  string
}

type Producer struct {
	producer.Config

	ProducerID       string
	OverseerBrokerID string
	CarrierBrokerID  string
}

type Repository interface {
	AddTopic(ctx context.Context, topic Topic) error
	UpdateTopic(ctx context.Context, topic Topic) error
	GetTopic(ctx context.Context, topicID string) (Topic, error)
	RemoveTopic(ctx context.Context, topicID string) error

	AddConsumer(ctx context.Context, consumer Consumer) error
	UpdateConsumer(ctx context.Context, consumer Consumer) error
	GetConsumer(ctx context.Context, consumerID string) (Consumer, error)
	RemoveConsumer(ctx context.Context, consumerID string) error

	AddProducer(ctx context.Context, producer Producer) error
	UpdateProducer(ctx context.Context, producer Producer) error
	GetProducer(ctx context.Context, producerID string) (Producer, error)
	RemoveProducer(ctx context.Context, producerID string) error
}
