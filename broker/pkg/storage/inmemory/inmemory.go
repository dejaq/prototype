package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	storage "github.com/bgadrian/dejaq-broker/broker/pkg/storage/timeline"
	derrors "github.com/bgadrian/dejaq-broker/common/errors"
	dtime "github.com/bgadrian/dejaq-broker/common/time"
	"github.com/bgadrian/dejaq-broker/common/timeline"
)

//force implementation of interface
var _ = storage.Repository(&InMemory{})

type InMemory struct {
	tmp   map[uint16]map[string]timeline.Message
	mutex sync.RWMutex
}

func NewInMemory() *InMemory {
	return &InMemory{tmp: make(map[uint16]map[string]timeline.Message)}
}

func (m *InMemory) Insert(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	//log.Println("insert messages into timeline", string(timelineID))
	for _, msg := range msgs {
		if _, ok := m.tmp[msg.BucketID]; !ok {
			m.tmp[msg.BucketID] = make(map[string]timeline.Message)
		}
		m.tmp[msg.BucketID][msg.GetID()] = msg
	}
	return nil
}

func (m *InMemory) Select(ctx context.Context, timelineID []byte, ranges []domain.BucketRange, limit int, maxTimestamp uint64) ([]timeline.Message, bool, error) {
	//log.Println("select messages from timeline", string(timelineID))
	var result []timeline.Message
	for bri := range ranges {
		for bIndex := ranges[bri].Min(); bIndex <= ranges[bri].Max(); bIndex++ {
			tmpResult, hasMore, err := m.selectFromBucket(ctx, bIndex, limit)
			if err != nil {
				return result, hasMore, err
			}
			result = append(result, tmpResult...)
			if len(result) >= limit {
				return result, true, nil
			}
		}
	}

	return result, false, nil
}

func (m *InMemory) selectFromBucket(ctx context.Context, bucket uint16, limit int) ([]timeline.Message, bool, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	nowMS := dtime.TimeToMS(time.Now().UTC())
	var result []timeline.Message
	for i := range m.tmp[bucket] {
		if m.tmp[bucket][i].TimestampMS > nowMS {
			continue
		}
		if limit <= 0 {
			break
		}
		limit--
		result = append(result, m.tmp[bucket][i])
	}

	return result, len(result) < len(m.tmp[bucket]), nil
}

func (m *InMemory) Delete(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//log.Println("delete messages from timeline", string(timelineID))
	for mi := range msgs {
		delete(m.tmp[msgs[mi].BucketID], msgs[mi].GetID())
	}
	return nil
}

//------------------------------- We do not need these for now
func (m *InMemory) Update(ctx context.Context, timelineID []byte, messageTimestamps []storage.MsgTime) []derrors.MessageIDTuple {
	return nil
}

func (m *InMemory) UpdateLeases(ctx context.Context, timelineID []byte, msgs []timeline.Message) []derrors.MessageIDTuple {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, msg := range msgs {
		m.tmp[msg.BucketID][msg.GetID()] = msg
	}
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
