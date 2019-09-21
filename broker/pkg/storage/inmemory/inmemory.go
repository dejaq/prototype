package inmemory

import (
	"context"

	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

//force implementation of interface
var _ = storage.Repository(&Inmemory{})

type Inmemory struct {
	tmp map[string]timeline.Message
}

func (m *Inmemory) Insert(ctx context.Context, msgs []timeline.Message) []storage.MsgErr {
	for _, msg := range msgs {
		m.tmp[msg.GetID()] = msg
	}
	return nil
}
func (m *Inmemory) Select(ctx context.Context, buckets []int, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error) {
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
func (m *Inmemory) Delete(ctx context.Context, ids [][]byte) {
	for _, idAsByte := range ids {
		delete(m.tmp, string(idAsByte))
	}
}

//------------------------------- We do not need these for now
func (m *Inmemory) Update(context.Context, []storage.MsgTime) []storage.MsgErr { return nil }

//LOOKUP message by messageID (owner control, lease operations)
func (m *Inmemory) Lookup(context.Context, [][]byte) ([]timeline.Message, []storage.MsgErr) {
	return nil, nil
}

//COUNT messages BY RANGE (spike detection/consumer scaling and metrics)
func (m *Inmemory) CountByRange(ctx context.Context, a, b uint64) uint64 { return 0 }

//COUNT messages BY RANGE and LockConsumerID is empty (count processing status)
func (m *Inmemory) CountByRangeProcessing(ctx context.Context, a, b uint64) uint64 { return 0 }

//COUNT messages BY RANGE and LockConsumerID is not empty (count waiting status)
func (m *Inmemory) CountByRangeWaiting(ctx context.Context, a, b uint64) uint64 { return 0 }

//SELECT messages by LockConsumerID (when consumer restarts)
func (m *Inmemory) SelectByConsumer(context.Context, string) []timeline.Message { return nil }

//SELECT messages by ProducerOwnerID (ownership control)
func (m *Inmemory) SelectByProducer(context.Context, string) []timeline.Message { return nil }
