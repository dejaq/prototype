package inmemory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/atomic"

	"github.com/sirupsen/logrus"

	"github.com/dejaq/prototype/broker/pkg/synchronization"

	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"

	"github.com/dejaq/prototype/broker/domain"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
)

//force implementation of interface
var _ = storage.Repository(&memory{})

var (
	ErrTopicNotExists                 = errors.New("topic not exists")
	ErrTopicAlreadyExists             = errors.New("topic already exists")
	ErrInvalidTopicName               = errors.New("invalid topic name")
	ErrBucketCountLessThanOne         = errors.New("buckets count should not be less than 1")
	ErrBucketNotExists                = errors.New("bucket not exists")
	ErrUnknownDeleteCaller            = errors.New("unknown delete caller")
	ErrCallerNotAllowToDeleteMessage  = errors.New("caller not allowed to delete message")
	ErrMessageNotFound                = errors.New("message not found in storage")
	ErrExpiredLease                   = errors.New("can not do actions on messages without an active lease")
	ErrMessageWrongVersion            = errors.New("message has a different version, maybe it was updated meantime")
	ErrMessageWithSameIDAlreadyExists = errors.New("message with same id already exists")
)

type memory struct {
	mu             sync.Mutex
	topics         map[string]topic
	catalog        synchronization.Catalog
	deleteReqCount atomic.Int64
}

type topic struct {
	id      string
	buckets []bucket
}

type bucket struct {
	mu       sync.Mutex
	id       uint16
	messages []message
}

type message struct {
	id         string
	endLeaseMS uint64
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

	m.mu.Lock()
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
	m.mu.Unlock()
	return nil
}

// Insert ...
func (m *memory) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	stringTimelineID := string(timelineID)
	var errs []derrors.MessageIDTuple

	// check if topic exists
	if _, ok := m.topics[stringTimelineID]; !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "insert"
		derror.Message = ErrTopicNotExists.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotExists
		return append(errs, derrors.MessageIDTuple{Error: derror})
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
			errs = append(errs, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// TODO maybe check more details about a message, like ID, BucketID, maybe all details
		// insert message
		bucket := &m.topics[stringTimelineID].buckets[int(msg.BucketID)]

		bucket.mu.Lock()
		// raise error if message with same id already exists
		var hasError bool
		for _, message := range bucket.messages {
			if bytes.Equal(message.data.ID, msg.ID) {
				// TODO will refactor this, it is a nasty way
				hasError = true
				var derror derrors.Dejaror
				derror.Module = derrors.ModuleStorage
				derror.Operation = "insert"
				derror.Message = ErrMessageWithSameIDAlreadyExists.Error()
				derror.ShouldRetry = false
				derror.WrappedErr = ErrMessageWithSameIDAlreadyExists
				errs = append(errs, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
				break
			}
		}

		if !hasError {
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
		bucket.mu.Unlock()
	}

	return errs
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

		bucket := &m.topics[stringTimelineID].buckets[int(bucketID)]

		// TODO issue, always will get messages from lower buckets which brake time priority, because of limit could not rich high priority messages
		// get available messages
		bucket.mu.Lock()
		for i, msg := range bucket.messages {
			// stop when rich limit
			if limit <= 0 {
				bucket.mu.Unlock()
				return results, false, nil
			}

			// messages with lease expired or messages unleased (prefetch)
			if msg.endLeaseMS <= currentTimeMS || (msg.endLeaseMS > currentTimeMS && msg.endLeaseMS <= maxTimeMS && len(msg.data.LockConsumerID) == 0) {
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
				m.topics[stringTimelineID].buckets[int(bucketID)].messages[i].data.LockConsumerID = consumerID

				results = append(results, timeline.Lease{
					ExpirationTimestampMS: endLeaseMS,
					ConsumerID:            consumerID,
					Message:               timeline.NewLeaseMessage(m.topics[stringTimelineID].buckets[int(bucketID)].messages[i].data),
				})
				limit--
			}
		}
		bucket.mu.Unlock()
	}

	return results, false, nil
}

// Lookup - not used at this time
func (m *memory) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

// Delete ...
func (m *memory) Delete(ctx context.Context, deleteMessages timeline.DeleteMessages) []derrors.MessageIDTuple {
	var errs []derrors.MessageIDTuple

	switch deleteMessages.CallerType {
	case timeline.DeleteCaller_Consumer:
		if delErr := m.deleteByConsumerId(deleteMessages); delErr != nil {
			errs = append(errs, delErr...)
		}
	case timeline.DeleteCaller_Producer:
		if delErr := m.deleteByProducerGroupId(deleteMessages); delErr != nil {
			errs = append(errs, delErr...)
		}
	default:
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "delete"
		derror.Message = fmt.Sprintf("could not identify hwo wants to delete messages")
		derror.ShouldRetry = false
		derror.WrappedErr = ErrUnknownDeleteCaller
		logrus.WithError(derror)
		errs = append(errs, derrors.MessageIDTuple{Error: derror})
	}
	return errs
}

func (m *memory) deleteByConsumerId(deleteMessages timeline.DeleteMessages) []derrors.MessageIDTuple {
	m.deleteReqCount.Add(int64(len(deleteMessages.Messages)))
	//fmt.Printf("-- Requested messages to delete %d\n", mu.deleteReqCount)

	stringTimelineID := string(deleteMessages.TimelineID)
	var errs []derrors.MessageIDTuple

	// check if topic exists
	if _, ok := m.topics[stringTimelineID]; !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "delete"
		derror.Message = ErrTopicNotExists.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotExists
		return append(errs, derrors.MessageIDTuple{Error: derror})
	}

	for _, deleteMessage := range deleteMessages.Messages {
		// check if bucket exists
		if len(m.topics[stringTimelineID].buckets)-1 < int(deleteMessage.BucketID) {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = ErrBucketNotExists.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrBucketNotExists
			errs = append(errs, derrors.MessageIDTuple{MessageID: deleteMessage.MessageID, Error: derror})
		}

		indexToDelete := -1
		bucket := &m.topics[stringTimelineID].buckets[int(deleteMessage.BucketID)]
		bucket.mu.Lock()
		for i, msg := range bucket.messages {
			if !bytes.Equal(msg.data.ID, deleteMessage.MessageID) {
				continue
			}
			indexToDelete = i
			break
		}

		// raise error if did not find message
		if indexToDelete < 0 {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = fmt.Sprintf("can not delete on behalf of: %v with id: %s from timeline: %s, err: %s",
				deleteMessages.CallerType,
				deleteMessages.CallerID,
				deleteMessages.TimelineID,
				ErrMessageNotFound.Error(),
			)
			derror.ShouldRetry = false
			derror.WrappedErr = ErrMessageNotFound
			errs = append(errs, derrors.MessageIDTuple{MessageID: deleteMessage.MessageID, Error: derror})
			logrus.WithError(derror)

			bucket.mu.Unlock()
			continue
		}

		msg := m.topics[stringTimelineID].buckets[int(deleteMessage.BucketID)].messages[indexToDelete]

		// raise error if message does not belongs to consumer
		if !bytes.Equal(msg.data.LockConsumerID, deleteMessages.CallerID) {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = fmt.Sprintf("can not delete on behalf of: %v with id: %s from timeline: %s, err: %s",
				deleteMessages.CallerType,
				deleteMessages.CallerID,
				deleteMessages.TimelineID,
				ErrCallerNotAllowToDeleteMessage.Error(),
			)
			derror.ShouldRetry = false
			derror.WrappedErr = ErrCallerNotAllowToDeleteMessage
			errs = append(errs, derrors.MessageIDTuple{MessageID: deleteMessage.MessageID, Error: derror})
			logrus.WithError(derror)

			bucket.mu.Unlock()
			continue
		}

		// raise error if consumer lease is expired
		if deleteMessages.Timestamp > msg.endLeaseMS {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = fmt.Sprintf("can not delete on behalf of: %v with id: %s from timeline: %s, err: %s",
				deleteMessages.CallerType,
				deleteMessages.CallerID,
				deleteMessages.TimelineID,
				ErrExpiredLease.Error(),
			)
			derror.ShouldRetry = false
			derror.WrappedErr = ErrExpiredLease
			errs = append(errs, derrors.MessageIDTuple{MessageID: deleteMessage.MessageID, Error: derror})
			logrus.WithError(derror)

			bucket.mu.Unlock()
			continue
		}

		// raise error if version is not the same
		if deleteMessage.Version != msg.data.Version {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = fmt.Sprintf("can not delete on behalf of: %v with id: %s from timeline: %s, err: %s",
				deleteMessages.CallerType,
				deleteMessages.CallerID,
				deleteMessages.TimelineID,
				ErrMessageWrongVersion.Error(),
			)
			derror.ShouldRetry = false
			derror.WrappedErr = ErrMessageWrongVersion
			errs = append(errs, derrors.MessageIDTuple{MessageID: deleteMessage.MessageID, Error: derror})
			logrus.WithError(derror)

			bucket.mu.Unlock()
			continue
		}

		// delete message
		bucket.messages = removeMessage(bucket.messages, indexToDelete)
		bucket.mu.Unlock()

		//fmt.Printf("Delete message: %s from: %d\n",
		//	deleteMessage.MessageID,
		//	len(bucket.messages),
		//)
	}

	return errs
}

func (m *memory) deleteByProducerGroupId(deleteMessages timeline.DeleteMessages) []derrors.MessageIDTuple {
	var deleteErrors []derrors.MessageIDTuple
	// TODO implement when will rich this part
	return deleteErrors
}

func removeMessage(s []message, i int) []message {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
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
