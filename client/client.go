package client

import (
	"context"

	"github.com/bgadrian/dejaq-broker/client/timeline/consumer"
	"github.com/bgadrian/dejaq-broker/client/timeline/producer"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

// Overseer is the gateway to access general commands
type Overseer interface {
	CreateTimelineTopic(ctx context.Context, id string, topicSettings timeline.TopicSettings) error
	//TopicStatus()
	//ClusterInfo()
}

// Client is the main window to the brokers
type Client interface {
	NewOverseerClient() Overseer
	NewConsumer(*consumer.Config) *consumer.Consumer
	NewProducer(*producer.Config) *producer.Producer
	Close()
}
