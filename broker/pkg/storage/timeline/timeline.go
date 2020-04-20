package timeline

import (
	"context"

	"github.com/dejaq/prototype/broker/domain"

	"github.com/dejaq/prototype/common/timeline"
)

type MsgTime struct {
	MessageID    []byte
	NewTimestamp uint64
}

// The representation of a Timeline storage.
// See the architecture docs on the design principles and for what actions are responsible it has.
type Repository interface {
	// Returns a Dejaror
	CreateTopic(ctx context.Context, timelineID string) error
	// INSERT messages in a timeline
	// Returns a Dejaror or a MessageIDTupleList (if only specific messages failed)
	Insert(ctx context.Context, req timeline.InsertMessagesRequest) error
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
	// DELETE remove message(s) from storage
	// Only CONSUMER that have an active lease can delete a message
	// Only PRODUCER that own message, message is not leased by a CONSUMER can delete it
	// Lease is implemented at storage level
	// Returns a Dejaror or a MessageIDTupleList (if only specific messages failed)
	Delete(ctx context.Context, req timeline.DeleteMessagesRequest) error
	// SelectByConsumer return consumer associated messages already leased
	SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, limit int, maxTimestamp uint64) ([]timeline.Lease, bool, error)
}
