package carrier

import (
	"sync"
	"unsafe"

	"github.com/dejaq/prototype/broker/domain"
	"github.com/dejaq/prototype/common/protocol"
)

type Consumer struct {
	id []byte

	mu              sync.RWMutex
	assignedBuckets []domain.BucketRange
	status          protocol.ConsumerStatus
	hydrateStatus   protocol.HydrationStatus

	Topic   string
	cluster string
	leaseMs uint64
}

func NewConsumer(ID []byte, topic string, cluster string, leaseMs uint64) *Consumer {
	return &Consumer{id: ID, Topic: topic, cluster: cluster, leaseMs: leaseMs}
}

func (c *Consumer) SetAssignedBuckets(bucketRange []domain.BucketRange) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assignedBuckets = bucketRange
}

func (c *Consumer) GetAssignedBuckets() []domain.BucketRange {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.assignedBuckets
}

func (c *Consumer) SetHydrateStatus(hydrateStatus protocol.HydrationStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hydrateStatus = hydrateStatus
}

func (c *Consumer) GetHydrateStatus() protocol.HydrationStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hydrateStatus
}

func (c *Consumer) GetID() string {
	return *(*string)(unsafe.Pointer(&c.id))
}

func (c *Consumer) GetIDAsBytes() []byte {
	return *(*[]byte)(unsafe.Pointer(&c.id))
}

// GetTopic ...
func (c *Consumer) GetTopic() string {
	return c.Topic
}

func (c *Consumer) GetTopicAsBytes() []byte {
	return *(*[]byte)(unsafe.Pointer(&c.Topic))
}

func (c *Consumer) GetCluster() string {
	return c.cluster
}

func (c *Consumer) GetLeaseMs() uint64 {
	return c.leaseMs
}

func (c *Consumer) SetStatus(status protocol.ConsumerStatus) {
	c.mu.Lock()
	c.status = status
	c.mu.Unlock()
}

func (c *Consumer) LoadAvailableBufferSize() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status.AvailableBufferSize
}

func (c *Consumer) AddAvailableBufferSize(delta uint32) {
	c.mu.Lock()
	c.status.AvailableBufferSize += delta
	c.mu.Unlock()
}
