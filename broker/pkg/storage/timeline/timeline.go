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
	//	INSERT messages ([]messages) map[msgID]error
	Insert(context.Context, []timeline.Message) []MsgErr
	//SELECT message.ID BY BUCKETIDs ([]bucketIDs, limit, maxTimestamp) ([]messages, hasMore, error)
	Select(ctx context.Context, buckets []int, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error)
	//UPDATE timestamp BY MESSAGEIDs (map[msgID]newTimestamp])  map[msgID]error (for extend/release)
	Update(context.Context, []MsgTime) []MsgErr
	//LOOKUP message by messageID (owner control, lease operations)
	Lookup(context.Context, [][]byte) ([]timeline.Message, []MsgErr)
	//DELETE messages by messageID map[msgID]error
	Delete(context.Context, [][]byte)
	//COUNT messages BY RANGE (spike detection/consumer scaling and metrics)
	CountByRange(ctx context.Context, a, b uint64) uint64
	//COUNT messages BY RANGE and LockConsumerID is empty (count processing status)
	CountByRangeProcessing(ctx context.Context, a, b uint64) uint64
	//COUNT messages BY RANGE and LockConsumerID is not empty (count waiting status)
	CountByRangeWaiting(ctx context.Context, a, b uint64) uint64
	//SELECT messages by LockConsumerID (when consumer restarts)
	SelectByConsumer(context.Context, string) []timeline.Message
	//SELECT messages by ProducerOwnerID (ownership control)
	SelectByProducer(context.Context, string) []timeline.Message
}
