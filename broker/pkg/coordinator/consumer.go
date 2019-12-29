package coordinator

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
	hydrateStatus   protocol.HydrationStatus

	topic   string
	cluster string
	leaseMs uint64
}

func NewConsumer(ID []byte, topic string, cluster string, leaseMs uint64) *Consumer {
	return &Consumer{id: ID, topic: topic, cluster: cluster, leaseMs: leaseMs}
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
	return c.topic
}

func (c *Consumer) GetTopicAsBytes() []byte {
	return *(*[]byte)(unsafe.Pointer(&c.topic))
}

func (c *Consumer) GetCluster() string {
	return c.cluster
}

func (c *Consumer) GetLeaseMs() uint64 {
	return c.leaseMs
}
