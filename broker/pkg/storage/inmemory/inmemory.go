package inmemory

import (
	"context"
	"errors"
	"sort"

	"github.com/dejaq/prototype/broker/pkg/synchronization"

	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"

	"github.com/dejaq/prototype/broker/domain"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
)

//force implementation of interface
var _ = storage.Repository(&memory{})

var (
	ErrTopicNotExists         = errors.New("topic not exists")
	ErrTopicAlreadyExists     = errors.New("topic already exists")
	ErrInvalidTopicName       = errors.New("invalid topic name")
	ErrBucketCountLessThanOne = errors.New("buckets count should not be less than 1")
	ErrBucketNotExists        = errors.New("bucket not exists")
)

type memory struct {
	topics  map[string]topic
	catalog synchronization.Catalog
}

type topic struct {
	id      string
	buckets []bucket
}

type bucket struct {
	id       uint16
	messages []message
}

type message struct {
	id         string
	endLeaseMS uint64
	consumerID string
	data       timeline.Message
}

func New(catalog synchronization.Catalog) *memory {
	return &memory{
		topics:  make(map[string]topic),
		catalog: catalog,
	}
}

func (m *memory) CreateTopic(ctx context.Context, timelineID string) error {
	if timelineID == "" {
		return ErrInvalidTopicName
	}

	topicInfo, _ := m.catalog.GetTopic(ctx, timelineID)
	bucketCount := topicInfo.Settings.BucketCount

	if bucketCount < 1 {
		return ErrBucketCountLessThanOne
	}
	if _, ok := m.topics[timelineID]; ok {
		return ErrTopicAlreadyExists
	}
	var buckets []bucket
	for i := 0; i < int(bucketCount); i++ {
		buckets = append(buckets, bucket{})
	}
	m.topics[timelineID] = topic{
		id:      timelineID,
		buckets: buckets,
	}
	return nil
}

// Insert ...
func (m *memory) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	stringTimelineID := string(timelineID)
	var insertErrors []derrors.MessageIDTuple

	// check if bucket exists
	if _, ok := m.topics[stringTimelineID]; !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "insert"
		derror.Message = ErrTopicNotExists.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotExists
		return append(insertErrors, derrors.MessageIDTuple{Error: derror})
	}

	// iterate over messages and insert them
	for _, msg := range messages {
		// check if bucket exists
		if len(m.topics[stringTimelineID].buckets)-1 < int(msg.BucketID) {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "insert"
			derror.Message = ErrBucketNotExists.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrBucketNotExists
			insertErrors = append(insertErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// TODO maybe check more details about a message, like ID, BucketID, maybe all details
		// insert message
		bucket := &m.topics[stringTimelineID].buckets[int(msg.BucketID)]
		bucket.id = msg.BucketID
		bucket.messages = append(bucket.messages, message{
			id:         msg.GetID(),
			endLeaseMS: msg.TimestampMS,
			data:       msg,
		})

		// sort messages asc in slice
		sort.Slice(bucket.messages, func(i, j int) bool {
			return bucket.messages[i].data.TimestampMS < bucket.messages[j].data.TimestampMS
		})
	}

	return insertErrors
}

func (m *memory) GetAndLease(
	ctx context.Context,
	timelineID []byte,
	buckets domain.BucketRange,
	consumerID []byte,
	leaseMs uint64,
	limit int,
	currentTimeMS, maxTimeMS uint64,
) ([]timeline.Lease, bool, error) {
	stringTimelineID := string(timelineID)
	stringConsumerID := string(consumerID)
	var results []timeline.Lease

	// check if topic exists
	if _, ok := m.topics[stringTimelineID]; !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "getAndLease"
		derror.Message = ErrTopicNotExists.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotExists
		return results, false, derror
	}

	// range over buckets collection
	for bucketID := buckets.Min(); bucketID <= buckets.Max(); bucketID++ {
		// check if bucket exists
		if len(m.topics[stringTimelineID].buckets) < int(bucketID) {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "insert"
			derror.Message = ErrBucketNotExists.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrBucketNotExists
			return results, false, derror
		}

		// TODO issue, always will get messages from lower buckets which brake time priority, because of limit could not rich high priority messages
		// get available messages
		for i, msg := range m.topics[stringTimelineID].buckets[int(bucketID)].messages {
			// stop when rich limit
			if limit <= 0 {
				return results, false, nil
			}

			// messages with lease expired or messages unleased (prefetch)
			if msg.endLeaseMS <= currentTimeMS || (msg.endLeaseMS > currentTimeMS && msg.endLeaseMS <= maxTimeMS && msg.consumerID == "") {
				// calculate endLeaseMS by choose max time
				// Normal:   currentTimeMS + leaseMS
				// Prefetch: maxTimeMS + leaseMS
				timeReference := currentTimeMS
				if currentTimeMS < msg.endLeaseMS {
					timeReference = msg.endLeaseMS
				}
				endLeaseMS := timeReference + leaseMs

				// add or refresh lease on message
				m.topics[stringTimelineID].buckets[int(bucketID)].messages[i].endLeaseMS = endLeaseMS
				m.topics[stringTimelineID].buckets[int(bucketID)].messages[i].consumerID = stringConsumerID

				results = append(results, timeline.Lease{
					ExpirationTimestampMS: endLeaseMS,
					ConsumerID:            consumerID,
					Message:               timeline.NewLeaseMessage(m.topics[stringTimelineID].buckets[int(bucketID)].messages[i].data),
				})
				limit--
			}
		}
	}

	return results, false, nil
}

// Lookup - not used at this time
func (m *memory) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

// Delete ...
func (m *memory) Delete(ctx context.Context, deleteMessages timeline.DeleteMessages) []derrors.MessageIDTuple {
	var deleteErrors []derrors.MessageIDTuple
	return deleteErrors
}

// CountByRange - not used at this time
func (m *memory) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// CountByRangeProcessing - not used at this time
func (m *memory) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// CountByRangeWaiting - not used at this time
func (m *memory) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// SelectByConsumer...
func (m *memory) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, limit int, timeReferenceMS uint64) ([]timeline.Lease, bool, error) {
	var results []timeline.Lease
	return results, false, nil
}

// SelectByProducer - not used at this time
func (m *memory) SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message {
	return nil
}
