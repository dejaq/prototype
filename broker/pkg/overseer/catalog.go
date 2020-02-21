package overseer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dejaq/prototype/broker/pkg/synchronization"
)

var onceCatalog sync.Once
var defaultCatalog synchronization.Catalog

func GetDefaultCatalog() synchronization.Catalog {
	onceCatalog.Do(func() {
		defaultCatalog = newCatalog()
	})
	return defaultCatalog
}

type Catalog struct {
	m         sync.RWMutex
	topics    map[string]synchronization.Topic
	consumers map[string]synchronization.Consumer
	producers map[string]synchronization.Producer
}

func newCatalog() synchronization.Catalog {
	return &Catalog{
		topics:    make(map[string]synchronization.Topic),
		consumers: make(map[string]synchronization.Consumer),
		producers: make(map[string]synchronization.Producer),
	}
}

func (c *Catalog) AddTopic(ctx context.Context, topic synchronization.Topic) error {
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.topics[topic.ID]; ok {
		return errors.New("topics already exists")
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

func (c *Catalog) GetTopic(ctx context.Context, topicID string) (synchronization.Topic, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	topic, ok := c.topics[topicID]
	if !ok {
		return synchronization.Topic{}, errors.New("topic not found")
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
