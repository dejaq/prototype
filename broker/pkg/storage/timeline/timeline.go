package timeline

import (
	"context"

	"github.com/bgadrian/dejaq-broker/broker/domain"

	"github.com/bgadrian/dejaq-broker/common/errors"

	"github.com/bgadrian/dejaq-broker/common/timeline"
)

type MsgTime struct {
	MessageID    []byte
	NewTimestamp uint64
}

type Repository interface {
	// Create a sample interface
	CreateTopic(ctx context.Context, timelineID string) error
	// INSERT messages (timelineID, []messages) map[msgID]error
	Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []errors.MessageIDTuple
	// GetAndLease message.ID BY TimelineID, BucketIDs ([]bucketIDs, limit, maxTimestamp) ([]messages, hasMore, error)
	// Get messages from storage and apply Lease on them
	GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId string, leaseMs uint64, limit int, maxTimestamp uint64) ([]timeline.PushLeases, bool, error)
	// LOOKUP message by TimelineID, MessageID (owner control, lease operations)
	Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []errors.MessageIDTuple)
	// DELETE messages by TimelineID, MessageID map[msgID]error
	Delete(ctx context.Context, timelineID []byte, messageIDs []timeline.Message) []errors.MessageIDTuple
	// COUNT messages BY TimelineID, RANGE (spike detection/consumer scaling and metrics)
	CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64
	// COUNT messages BY TimelineID, RANGE and LockConsumerID is empty (count processing status)
	CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64
	// COUNT messages BY TimelineID, RANGE and LockConsumerID is not empty (count waiting status)
	CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64
	// SELECT messages by TimelineID, LockConsumerID (when consumer restarts)
	SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message
	// SELECT messages by TimelineID, ProducerOwnerID (ownership control)
	SelectByProducer(ctx context.Context, timelineID []byte, producrID []byte) []timeline.Message
}
