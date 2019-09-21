package timeline

import (
	"context"

	"github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

type MsgErr struct {
	MessageID []byte
	Error     errors.Dejaror
}

type MsgTime struct {
	MessageID    []byte
	NewTimestamp uint64
}

type Repository interface {
	// INSERT messages (timelineID, []messages) map[msgID]error
	Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []MsgErr
	// SELECT message.ID BY TimelineID, BucketIDs ([]bucketIDs, limit, maxTimestamp) ([]messages, hasMore, error)
	Select(ctx context.Context, timelineID []byte, buckets []int, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error)
	// UPDATE timestamp BY TimelineID, MessageIDs (map[msgID]newTimestamp])  map[msgID]error (for extend/release)
	Update(ctx context.Context, timelineID []byte, messageTimestamps []MsgTime) []MsgErr
	// LOOKUP message by TimelineID, MessageID (owner control, lease operations)
	Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []MsgErr)
	// DELETE messages by TimelineID, MessageID map[msgID]error
	Delete(ctx context.Context, timelineID []byte, messageIDs [][]byte)
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
