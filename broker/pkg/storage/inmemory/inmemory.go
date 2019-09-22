package inmemory

import (
	"context"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	derrors "github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

//force implementation of interface
var _ = storage.Repository(&InMemory{})

type InMemory struct {
	tmp map[string]timeline.Message
}

func NewInMemory() *InMemory {
	return &InMemory{tmp: make(map[string]timeline.Message)}
}

func (m *InMemory) Insert(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	for _, msg := range msgs {
		m.tmp[msg.GetID()] = msg
	}
	return nil
}

func (m *InMemory) Select(ctx context.Context, timelineID []byte, buckets []int, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error) {
	var result []timeline.Message

	for _, msg := range m.tmp {
		if limit <= 0 {
			break
		}
		limit--
		result = append(result, msg)
	}

	return result, len(result) < len(m.tmp), nil
}

func (m *InMemory) Delete(ctx context.Context, timelineID []byte, ids [][]byte) {
	for _, idAsByte := range ids {
		delete(m.tmp, string(idAsByte))
	}
}

//------------------------------- We do not need these for now
func (m *InMemory) Update(ctx context.Context, timelineID []byte, messageTimestamps []storage.MsgTime) []derrors.MessageIDTuple {
	return nil
}

//LOOKUP message by messageID (owner control, lease operations)
func (m *InMemory) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

//COUNT messages BY RANGE (spike detection/consumer scaling and metrics)
func (m *InMemory) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 { return 0 }

//COUNT messages BY RANGE and LockConsumerID is empty (count processing status)
func (m *InMemory) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

//COUNT messages BY RANGE and LockConsumerID is not empty (count waiting status)
func (m *InMemory) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

//SELECT messages by LockConsumerID (when consumer restarts)
func (m *InMemory) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message {
	return nil
}

//SELECT messages by ProducerOwnerID (ownership control)
func (m *InMemory) SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message {
	return nil
}
