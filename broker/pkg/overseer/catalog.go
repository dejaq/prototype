package overseer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	derrors "github.com/dejaq/prototype/common/errors"

	"github.com/dejaq/prototype/broker/pkg/synchronization"
)

type Catalog struct {
	m         sync.RWMutex
	topics    map[string]synchronization.Topic
	consumers map[string]synchronization.Consumer
	producers map[string]synchronization.Producer
}

func NewCatalog() *Catalog {
	return &Catalog{
		topics:    make(map[string]synchronization.Topic),
		consumers: make(map[string]synchronization.Consumer),
		producers: make(map[string]synchronization.Producer),
	}
}
func (c *Catalog) GetAllTopics(ctx context.Context) ([]synchronization.Topic, error) {
	c.m.Lock()
	defer c.m.Unlock()
	result := make([]synchronization.Topic, 0, len(c.topics))

	for _, topic := range c.topics {
		result = append(result, topic)
	}
	return result, nil
}
func (c *Catalog) AddTopic(ctx context.Context, topic synchronization.Topic) error {
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.topics[topic.ID]; ok {
		return nil
	}

	c.topics[topic.ID] = topic

	return nil
}

func (c *Catalog) UpdateTopic(ctx context.Context, topic synchronization.Topic) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.topics[topic.ID] = topic
	return nil
}

// Returns a topic. If auto create is enabled and the topic is not found it will create one
func (c *Catalog) GetTopic(ctx context.Context, topicID string) (synchronization.Topic, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	topic, ok := c.topics[topicID]
	if !ok {
		return synchronization.Topic{}, derrors.ErrTopicNotFound
	}
	return topic, nil
}

func (c *Catalog) RemoveTopic(ctx context.Context, topicID string) error {
	c.m.Lock()
	defer c.m.Unlock()
	delete(c.topics, topicID)
	return nil
}

func (c *Catalog) AddConsumer(ctx context.Context, consumer synchronization.Consumer) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.consumers[getClientKey(consumer.Topic, consumer.ConsumerID)] = consumer
	return nil
}

func (c *Catalog) UpdateConsumer(ctx context.Context, consumer synchronization.Consumer) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.consumers[getClientKey(consumer.Topic, consumer.ConsumerID)] = consumer
	return nil
}

func (c *Catalog) GetConsumer(ctx context.Context, topic, consumerID string) (synchronization.Consumer, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	consumer, ok := c.consumers[getClientKey(topic, consumerID)]
	if !ok {
		return synchronization.Consumer{}, errors.New("consumer not found")
	}
	return consumer, nil
}

func (c *Catalog) RemoveConsumer(ctx context.Context, topic, consumerID string) error {
	c.m.Lock()
	defer c.m.Unlock()
	delete(c.consumers, getClientKey(topic, consumerID))
	return nil
}

func (c *Catalog) AddProducer(ctx context.Context, producer synchronization.Producer) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.producers[getClientKey(producer.Topic, producer.ProducerID)] = producer
	return nil
}

func (c *Catalog) UpdateProducer(ctx context.Context, producer synchronization.Producer) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.producers[getClientKey(producer.Topic, producer.ProducerID)] = producer
	return nil
}

func (c *Catalog) GetProducer(ctx context.Context, topic, producerID string) (synchronization.Producer, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	producer, ok := c.producers[(getClientKey(topic, producerID))]
	if !ok {
		return synchronization.Producer{}, errors.New("consumer not found")
	}
	return producer, nil
}

func (c *Catalog) RemoveProducer(ctx context.Context, topic, producerID string) error {
	c.m.Lock()
	defer c.m.Unlock()
	delete(c.producers, getClientKey(topic, producerID))
	return nil
}

func getClientKey(topic, clientID string) string {
	return fmt.Sprintf("%s:%s", topic, clientID)
}
