package timeline

import (
	"context"

	"github.com/dejaq/prototype/broker/domain"

	"github.com/dejaq/prototype/common/errors"

	"github.com/dejaq/prototype/common/timeline"
)

type MsgTime struct {
	MessageID    []byte
	NewTimestamp uint64
}

// TODO inconsistent error management, sometimes return error and other times []errors

type Repository interface {
	// Create a sample interface
	CreateTopic(ctx context.Context, timelineID string) error
	// INSERT messages (timelineID, []messages) map[msgID]error
	Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []errors.MessageIDTuple
	// Get messages from storage and apply Lease on them
	// maxTimeMS -> maximum timestamp for prefetch messages
	// TODO add structure here
	GetAndLease(
		ctx context.Context,
		timelineID []byte,
		buckets domain.BucketRange,
		consumerId []byte,
		leaseMs uint64,
		limit int,
		currentTimeMS uint64,
		maxTimeMS uint64,
	) ([]timeline.Lease, bool, error)
	// LOOKUP message by TimelineID, MessageID (owner control, lease operations)
	Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []errors.MessageIDTuple)
	// DELETE remove message(s) from storage
	// Only CONSUMER that have an active lease can delete a message
	// Only PRODUCER that own message, message is not leased by a CONSUMER can delete it
	// Lease is implemented at storage level
	Delete(ctx context.Context, deleteMessages timeline.DeleteMessages) []errors.MessageIDTuple
	// COUNT messages BY TimelineID, RANGE (spike detection/consumer scaling and metrics)
	CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64
	// COUNT messages BY TimelineID, RANGE and LockConsumerID is empty (count processing status)
	CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64
	// COUNT messages BY TimelineID, RANGE and LockConsumerID is not empty (count waiting status)
	CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64
	// SelectByConsumer return consumer associated messages already leased
	SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, limit int, maxTimestamp uint64) ([]timeline.Lease, bool, error)
	// SELECT messages by TimelineID, ProducerOwnerID (ownership control)
	SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message
}
