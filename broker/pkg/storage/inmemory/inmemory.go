package inmemory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	dtime "github.com/dejaq/prototype/common/time"

	"go.uber.org/atomic"

	"github.com/sirupsen/logrus"

	"github.com/dejaq/prototype/broker/pkg/synchronization"

	storage "github.com/dejaq/prototype/broker/pkg/storage/timeline"

	"github.com/dejaq/prototype/broker/domain"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
)

//force implementation of interface
var _ = storage.Repository(&Memory{})

var (
	ErrTopicNotFound                  = errors.New("topic not exists")
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

type Memory struct {
	mu             sync.Mutex
	topics         map[string]*topic
	catalog        synchronization.Catalog
	deleteReqCount atomic.Int64
}

type topic struct {
	id      string
	buckets []bucket
}

type bucket struct {
	m        sync.Mutex
	id       uint16
	messages []message
}

type message struct {
	id         string
	endLeaseMS uint64
	data       timeline.Message
}

func New(catalog synchronization.Catalog) *Memory {
	return &Memory{
		topics:  make(map[string]*topic),
		catalog: catalog,
	}
}

func (m *Memory) getTopic(timelineID string) (*topic, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic, ok := m.topics[timelineID]
	return topic, ok
}

func (m *Memory) CreateTopic(ctx context.Context, timelineID string) error {
	if timelineID == "" {
		return ErrInvalidTopicName
	}

	topicInfo, _ := m.catalog.GetTopic(ctx, timelineID)
	bucketCount := topicInfo.Settings.BucketCount

	if bucketCount < 1 {
		return ErrBucketCountLessThanOne
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[timelineID]; ok {
		return nil
	}
	var buckets []bucket
	for i := 0; i < int(bucketCount); i++ {
		buckets = append(buckets, bucket{})
	}
	m.topics[timelineID] = &topic{
		id:      timelineID,
		buckets: buckets,
	}
	return nil
}

// Insert ...
func (m *Memory) Insert(ctx context.Context, req timeline.InsertMessagesRequest) error {
	errs := make(derrors.MessageIDTupleList, 0)

	topic, ok := m.getTopic(req.GetTimelineID())

	// check if topic exists
	if !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "insert"
		derror.Message = ErrTopicNotFound.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotFound
		return append(errs, derrors.MessageIDTuple{MsgError: derror})
	}

	// iterate over messages and insert them
	for _, msg := range req.Messages {
		// check if bucket exists
		if len(topic.buckets)-1 < int(msg.BucketID) {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "insert"
			derror.Message = ErrBucketNotExists.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrBucketNotExists
			errs = append(errs, derrors.MessageIDTuple{MsgID: msg.ID, MsgError: derror})
		}

		// TODO maybe check more details about a message, like ID, BucketID, maybe all details
		// insert message
		bucket := &topic.buckets[int(msg.BucketID)]

		bucket.m.Lock()
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
				errs = append(errs, derrors.MessageIDTuple{MsgID: msg.ID, MsgError: derror})
				break
			}
		}

		if !hasError {
			bucket.messages = append(bucket.messages, message{
				id:         msg.GetID(),
				endLeaseMS: msg.TimestampMS,
				data:       insertMsgToMsg(msg, req.ProducerGroupID),
			})

			// sort messages asc in slice
			sort.Slice(bucket.messages, func(i, j int) bool {
				return bucket.messages[i].data.TimestampMS < bucket.messages[j].data.TimestampMS
			})
		}
		bucket.m.Unlock()
	}

	return errs
}

func (m *Memory) GetAndLease(
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

	topic, ok := m.getTopic(stringTimelineID)

	// check if topic exists
	if !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "getAndLease"
		derror.Message = ErrTopicNotFound.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotFound
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

		bucket := &topic.buckets[int(bucketID)]

		// TODO issue, always will get messages from lower buckets which brake time priority, because of limit could not rich high priority messages
		// get available messages
		bucket.m.Lock()
		for i, msg := range bucket.messages {
			// stop when rich limit
			if limit <= 0 {
				bucket.m.Unlock()
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
					Message:               newLeaseMessage(m.topics[stringTimelineID].buckets[int(bucketID)].messages[i].data),
				})
				limit--
			}
		}
		bucket.m.Unlock()
	}

	return results, false, nil
}

// Delete ...
func (m *Memory) Delete(ctx context.Context, deleteMessages timeline.DeleteMessagesRequest) error {
	errs := make(derrors.MessageIDTupleList, 0)

	switch deleteMessages.CallerType {
	case timeline.DeleteCallerConsumer:
		if delErr := m.deleteByConsumerId(deleteMessages); delErr != nil {
			errs = append(errs, delErr...)
		}
	case timeline.DeleteCallerProducer:
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
		errs = append(errs, derrors.MessageIDTuple{MsgError: derror})
	}
	return errs
}

func (m *Memory) deleteByConsumerId(deleteMessages timeline.DeleteMessagesRequest) []derrors.MessageIDTuple {
	m.deleteReqCount.Add(int64(len(deleteMessages.Messages)))
	//fmt.Printf("-- Requested messages to delete %d\n", mu.deleteReqCount)

	stringTimelineID := string(deleteMessages.TimelineID)
	var errs []derrors.MessageIDTuple

	topic, ok := m.getTopic(stringTimelineID)

	// check if topic exists
	if !ok {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "delete"
		derror.Message = ErrTopicNotFound.Error()
		derror.ShouldRetry = false
		derror.WrappedErr = ErrTopicNotFound
		return append(errs, derrors.MessageIDTuple{MsgError: derror})
	}

	for _, deleteMessage := range deleteMessages.Messages {
		// check if bucket exists
		if len(topic.buckets)-1 < int(deleteMessage.BucketID) {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = ErrBucketNotExists.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrBucketNotExists
			errs = append(errs, derrors.MessageIDTuple{MsgID: deleteMessage.MessageID, MsgError: derror})
		}

		indexToDelete := -1
		bucket := &topic.buckets[int(deleteMessage.BucketID)]
		bucket.m.Lock()
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
			errs = append(errs, derrors.MessageIDTuple{MsgID: deleteMessage.MessageID, MsgError: derror})
			logrus.WithError(derror)

			bucket.m.Unlock()
			continue
		}

		msg := topic.buckets[int(deleteMessage.BucketID)].messages[indexToDelete]

		// raise error if message does not belongs to consumer
		if string(msg.data.LockConsumerID) != deleteMessages.CallerID {
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
			errs = append(errs, derrors.MessageIDTuple{MsgID: deleteMessage.MessageID, MsgError: derror})
			logrus.WithError(derror)

			bucket.m.Unlock()
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
			errs = append(errs, derrors.MessageIDTuple{MsgID: deleteMessage.MessageID, MsgError: derror})
			logrus.WithError(derror)

			bucket.m.Unlock()
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
			errs = append(errs, derrors.MessageIDTuple{MsgID: deleteMessage.MessageID, MsgError: derror})
			logrus.WithError(derror)

			bucket.m.Unlock()
			continue
		}

		// delete message
		bucket.messages = removeMessage(bucket.messages, indexToDelete)
		bucket.m.Unlock()
	}

	return errs
}

func (m *Memory) deleteByProducerGroupId(deleteMessages timeline.DeleteMessagesRequest) []derrors.MessageIDTuple {
	var deleteErrors []derrors.MessageIDTuple
	// TODO implement when will rich this part
	return deleteErrors
}

func removeMessage(s []message, i int) []message {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

// SelectByConsumer...
func (m *Memory) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, limit int, timeReferenceMS uint64) ([]timeline.Lease, bool, error) {
	var results []timeline.Lease
	return results, false, nil
}

func insertMsgToMsg(i timeline.InsertMessagesRequestItem, producerGroupID string) timeline.Message {
	return timeline.Message{
		ID:              i.ID,
		TimestampMS:     i.TimestampMS,
		BodyID:          i.ID,
		Body:            i.Body,
		ProducerGroupID: []byte(producerGroupID),
		LockConsumerID:  nil,
		BucketID:        i.BucketID,
		Version:         i.Version,
	}
}

func newLeaseMessage(msg timeline.Message) timeline.MessageLease {
	return timeline.MessageLease{
		ID:              msg.ID,
		TimestampMS:     msg.TimestampMS,
		ProducerGroupID: msg.ProducerGroupID,
		Version:         msg.Version,
		Body:            msg.Body,
		BucketID:        msg.BucketID,
	}
}

func (m *Memory) CountByStatus(ctx context.Context, request timeline.CountRequest) (uint64, error) {
	topic, ok := m.getTopic(request.TimelineID)
	if !ok || topic == nil {
		return 0, ErrTopicNotFound
	}
	var result uint64
	now := dtime.TimeToMS(time.Now().UTC())
	for i := range topic.buckets {
		topic.buckets[i].m.Lock()
		for _, msg := range topic.buckets[i].messages {
			switch request.Type {
			case timeline.StatusAvailable:
				if msg.data.TimestampMS <= now {
					result++
				}
			case timeline.StatusWaiting:
				if msg.data.TimestampMS > now && len(msg.data.LockConsumerID) == 0 {
					result++
				}
			case timeline.StatusProcessing:
				if msg.data.TimestampMS > now && len(msg.data.LockConsumerID) != 0 {
					result++
				}
			default:
				topic.buckets[i].m.Unlock()
				return 0, errors.New("message status not implemented")
			}
		}
		topic.buckets[i].m.Unlock()
	}
	return result, nil
}
