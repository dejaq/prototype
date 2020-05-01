package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/sirupsen/logrus"

	"github.com/dejaq/prototype/broker/domain"
	derrors "github.com/dejaq/prototype/common/errors"
	"github.com/dejaq/prototype/common/timeline"
	"github.com/go-redis/redis"
)

type Client struct {
	host   string
	client *redis.Client
	// map[operationType]redisHash
	operationToScriptHash map[string]string
}

// TODO for demo purpose, to keep it in only one place, should be received as param by methods that use it
const clusterName = "demo"

// Errors
var (
	ErrMessageAlreadyExists         = errors.New("messageId already exists, you can not set it again")
	ErrOperationFailRollbackSuccess = errors.New("fail operation, rollback with success")
	ErrOperationFailRollbackFail    = errors.New("fail operation, rollback fail")
	ErrStorageInternalError         = errors.New("internal error on storage")
	ErrNotAllowedToPerformOperation = errors.New("not allowed to perform this operation")
	ErrUnknownWhoWantsToAction      = errors.New("could not identify hwo wants to do taht action")
	ErrUnknown                      = errors.New("no errors on storage level, no expected answer")
)

var operationToScript = map[string]string{
	"insert":          scripts.insert,
	"getAndLease":     scripts.getAndLease,
	"getByConsumerId": scripts.getByConsumerId,
	"consumerDelete":  scripts.consumerDelete,
}

// New ...
func New(host string) (*Client, error) {
	c := redis.NewClient(&redis.Options{Addr: host})
	_, err := c.Ping().Result()

	if err != nil {
		return nil, err
	}

	client := Client{
		host:   host,
		client: c,
	}

	// load operationToScript
	if err := client.loadScripts(operationToScript); err != nil {
		return nil, err
	}

	return &client, nil
}

func (c *Client) loadScripts(s map[string]string) error {
	hashes := make(map[string]string)
	for k, v := range s {
		hash, err := c.client.ScriptLoad(v).Result()
		if err != nil {
			return err
		}
		hashes[k] = hash
	}

	c.operationToScriptHash = hashes

	return nil
}

func (c *Client) createTimelineKey(clusterName string, timelineId []byte) string {
	return "dejaq::" + clusterName + "::" + string(timelineId)
}

func (c *Client) createBucketKey(clusterName string, timelineId []byte, bucketId uint16) string {
	idBucket := strconv.Itoa(int(bucketId))
	return c.createTimelineKey(clusterName, timelineId) + "::" + idBucket
}

func (c *Client) createMessageKey(clusterName string, timelineId []byte, bucketId uint16, messageId []byte) string {
	return c.createBucketKey(clusterName, timelineId, bucketId) + "::" + string(messageId)
}

func (c *Client) CreateTopic(ctx context.Context, timelineID string) error {
	// TODO add implementation
	return nil
}

// Insert ...
func (c *Client) Insert(ctx context.Context, req timeline.InsertMessagesRequest) error {
	// TODO create batches and insert

	insertErrors := make(derrors.MessageIDTupleList, 0)

	for mi := range req.Messages {
		bucketKey := c.createBucketKey(clusterName, []byte(req.TimelineID), req.Messages[mi].BucketID)
		messageKey := c.createMessageKey(clusterName, []byte(req.TimelineID), req.Messages[mi].BucketID, req.Messages[mi].ID)

		data := []string{
			"ID", req.Messages[mi].GetID(),
			"TimestampMS", strconv.FormatUint(req.Messages[mi].TimestampMS, 10),
			"BodyID", req.Messages[mi].GetID(),
			"Body", req.Messages[mi].GetBody(),
			"ProducerGroupID", req.GetProducerGroupID(),
			"LockConsumerID", "",
			"BucketID", strconv.Itoa(int(req.Messages[mi].BucketID)),
			"Version", strconv.Itoa(int(req.Messages[mi].Version)),
		}

		keys := []string{bucketKey, messageKey, data[1], data[3]}
		ok, err := c.client.EvalSha(c.operationToScriptHash["insert"], keys, data).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "insert"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = ErrStorageInternalError
			insertErrors = append(insertErrors, derrors.MessageIDTuple{MsgID: req.Messages[mi].ID, MsgError: derror})
		}

		// continue on success
		if ok == "0" {
			continue
		}

		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "insert"

		switch ok {
		// already exists
		case "1":
			derror.Message = ErrMessageAlreadyExists.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrMessageAlreadyExists
		// fail to add rollback with success
		case "2", "4":
			derror.Message = ErrOperationFailRollbackSuccess.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = ErrOperationFailRollbackSuccess
		// rollback fail, inconsistent data
		case "3":
			derror.Message = ErrOperationFailRollbackFail.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrOperationFailRollbackFail
		// unknown what is happen
		default:
			derror.Message = ErrUnknown.Error()
			derror.ShouldRetry = false
			derror.WrappedErr = ErrUnknown
		}

		insertErrors = append(insertErrors, derrors.MessageIDTuple{MsgID: req.Messages[mi].ID, MsgError: derror})
	}

	return insertErrors
}

// GetAndLease ...
func (c *Client) GetAndLease(
	ctx context.Context,
	timelineID []byte,
	buckets domain.BucketRange,
	consumerId []byte,
	leaseMs uint64,
	limit int,
	currentTimeMS, maxTimeMS uint64,
) ([]timeline.Lease, bool, error) {
	var results []timeline.Lease

	keys := []string{
		// timelineKey
		c.createTimelineKey(clusterName, timelineID),
		strconv.FormatUint(currentTimeMS, 10),
		strconv.FormatUint(maxTimeMS, 10),
		// lease duration on MS
		strconv.FormatUint(leaseMs, 10),
		// max number of messages to get
		strconv.Itoa(limit),
		// consumerId
		string(consumerId),
	}

	// slice of buckets ids
	var argv []string
	for bucketID := buckets.Min(); bucketID <= buckets.Max(); bucketID++ {
		argv = append(argv, strconv.Itoa(int(bucketID)))
	}

	data, err := c.client.EvalSha(c.operationToScriptHash["getAndLease"], keys, argv).Result()
	if err != nil {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "getAndLease"
		derror.Message = err.Error()
		derror.ShouldRetry = true
		derror.WrappedErr = ErrStorageInternalError
		return nil, false, derror
	}

	// TODO not the best practice, find a better solution here to remove interface mess
	dataCollection := data.([]interface{})
	for _, val := range dataCollection {
		data := val.([]interface{})

		exitCode := data[0].(string)

		// no success, be more verbose, you should know what`s happen, affected data,
		// how to repair in case of rollback failure
		if exitCode != "0" {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "getAndLease"

			switch exitCode {
			case "2":
				derror.Message = ErrOperationFailRollbackSuccess.Error()
				derror.ShouldRetry = true
				derror.WrappedErr = ErrOperationFailRollbackSuccess
			case "3":
				derror.Message = ErrOperationFailRollbackFail.Error()
				derror.ShouldRetry = true
				derror.WrappedErr = ErrOperationFailRollbackFail
			default:
				derror.Message = ErrUnknown.Error()
				derror.ShouldRetry = false
				derror.WrappedErr = ErrUnknown
			}

			logrus.WithError(derror).Errorf("redis error")
			continue
		}

		// till 1 indexes of data represents metadata
		timelineMessage, err := convertRawMsgToTimelineMsg(data[2:])
		if err != nil {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "getAndLease"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = err
			logrus.WithError(derror).Errorf("redis error")
			continue
		}

		endLeaseMS, err := strconv.ParseUint(data[1].(string), 10, 64)
		if err != nil {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "getAndLease"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = err
			logrus.WithError(derror).Errorf("redis error")
			continue
		}

		results = append(results, timeline.Lease{
			ExpirationTimestampMS: endLeaseMS,
			ConsumerID:            consumerId,
			Message:               newLeaseMessage(timelineMessage),
		})
	}

	return results, false, nil
}

func convertRawMsgToTimelineMsg(rawMessage []interface{}) (timeline.Message, error) {
	var message timeline.Message
	var tmp []string

	for i := 0; i <= 7; i++ {
		v, ok := rawMessage[i].(string)
		if !ok {
			return timeline.Message{}, errors.New("malformed message")
		}
		tmp = append(tmp, v)
	}

	if len(tmp) != 8 {
		return timeline.Message{}, errors.New("malformed message")
	}

	message.ID = *(*[]byte)(unsafe.Pointer(&tmp[0]))

	timestamp, err := strconv.ParseUint(tmp[1], 10, 64)
	if err != nil {
		return timeline.Message{}, err
	}
	message.TimestampMS = timestamp
	message.BodyID = *(*[]byte)(unsafe.Pointer(&tmp[2]))
	message.Body = *(*[]byte)(unsafe.Pointer(&tmp[3]))
	message.ProducerGroupID = *(*[]byte)(unsafe.Pointer(&tmp[4]))
	message.LockConsumerID = *(*[]byte)(unsafe.Pointer(&tmp[5]))

	bucketIdUint16, err := strconv.ParseUint(tmp[6], 10, 16)
	if err != nil {
		return timeline.Message{}, err
	}
	message.BucketID = uint16(bucketIdUint16)

	version, err := strconv.ParseUint(tmp[7], 10, 16)
	if err != nil {
		return timeline.Message{}, err
	}
	message.Version = uint16(version)

	return message, nil
}

// Delete ...
func (c *Client) Delete(ctx context.Context, deleteMessages timeline.DeleteMessagesRequest) error {
	deleteErrors := make(derrors.MessageIDTupleList, 0)
	keys := []string{
		c.createTimelineKey(clusterName, []byte(deleteMessages.TimelineID)),
		deleteMessages.CallerID,
		strconv.FormatUint(deleteMessages.Timestamp, 10),
	}

	switch deleteMessages.CallerType {
	case timeline.DeleteCallerConsumer:
		if delErr := deleteByConsumerId(c, deleteMessages, keys); delErr != nil {
			deleteErrors = append(deleteErrors, delErr...)
		}
	case timeline.DeleteCallerProducer:
		if delErr := deleteByProducerGroupId(c, deleteMessages, keys); delErr != nil {
			deleteErrors = append(deleteErrors, delErr...)
		}
	default:
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "delete"
		derror.Message = fmt.Sprintf("could not identify hwo wants to delete messages")
		derror.ShouldRetry = false
		derror.WrappedErr = ErrUnknownWhoWantsToAction
		logrus.WithError(derror)

		deleteErrors = append(deleteErrors, derrors.MessageIDTuple{MsgError: derror})
	}
	return deleteErrors
}

func deleteByConsumerId(c *Client, deleteMessages timeline.DeleteMessagesRequest, keys []string) []derrors.MessageIDTuple {
	var deleteErrors []derrors.MessageIDTuple
	var argv []string
	for _, msg := range deleteMessages.Messages {
		argv = append(argv, string(msg.MessageID))
		argv = append(argv, strconv.Itoa(int(msg.BucketID)))
		argv = append(argv, strconv.Itoa(int(msg.Version)))
	}
	data, err := c.client.EvalSha(c.operationToScriptHash["consumerDelete"], keys, argv).Result()
	if err != nil {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "delete"
		derror.Message = fmt.Sprintf("can not delete on behalf of: %v with id: %s from timeline: %s, err: %s",
			deleteMessages.CallerType,
			deleteMessages.CallerID,
			deleteMessages.TimelineID,
			err.Error(),
		)
		derror.ShouldRetry = true
		derror.WrappedErr = ErrStorageInternalError
		logrus.WithError(derror)
		return append(deleteErrors, derrors.MessageIDTuple{MsgError: derror})
	}
	dataCollection := data.([]interface{})
	for _, val := range dataCollection {
		v := val.([]interface{})
		code := v[1].(string)
		if code == "1" {
			messageId, _ := v[0].(string)
			endLeaseMS, ok := v[2].(string)
			if !ok {
				endLeaseMS = "-1"
			}

			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "delete"
			derror.Message = fmt.Sprintf("consumerID: %s does not have an active lease on messageID: %s at timeMS: %v on timelineID: %s his lease expired on: %s",
				deleteMessages.CallerID,
				messageId,
				deleteMessages.Timestamp,
				deleteMessages.TimelineID,
				endLeaseMS,
			)
			derror.ShouldRetry = false
			derror.WrappedErr = ErrNotAllowedToPerformOperation
			logrus.WithError(derror)

			deleteErrors = append(deleteErrors, derrors.MessageIDTuple{MsgError: derror})
		}
	}
	return deleteErrors
}

func deleteByProducerGroupId(c *Client, deleteMessages timeline.DeleteMessagesRequest, keys []string) []derrors.MessageIDTuple {
	// TODO will be implemented
	var deleteErrors []derrors.MessageIDTuple
	var argv []string
	data, err := c.client.EvalSha(c.operationToScriptHash["producerDelete"], keys, argv).Result()
	if err != nil {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "delete"
		derror.Message = fmt.Sprintf("can not delete on behalf of: %v with id: %s from timeline: %s, err: %s",
			deleteMessages.CallerType,
			deleteMessages.CallerID,
			deleteMessages.TimelineID,
			err.Error(),
		)
		derror.ShouldRetry = true
		derror.WrappedErr = ErrStorageInternalError
		logrus.WithError(derror)
		return []derrors.MessageIDTuple{{MsgError: derror}}
	}
	_ = data
	return deleteErrors
}

// SelectByConsumer return consumer associated messages already leased
func (c *Client) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, limit int, timeReferenceMS uint64) ([]timeline.Lease, bool, error) {
	var results []timeline.Lease

	keys := []string{
		c.createTimelineKey(clusterName, timelineID) + "::" + string(consumerID),
		strconv.Itoa(limit),
		strconv.FormatUint(timeReferenceMS, 10),
	}

	data, err := c.client.EvalSha(c.operationToScriptHash["getByConsumerId"], keys).Result()
	if err != nil {
		var derror derrors.Dejaror
		derror.Module = derrors.ModuleStorage
		derror.Operation = "getByConsumerId"
		derror.Message = err.Error()
		derror.ShouldRetry = true
		derror.WrappedErr = ErrStorageInternalError
		logrus.WithError(derror).Errorf("redis error")
		return nil, false, derror
	}

	dataCollection := data.([]interface{})
	for _, val := range dataCollection {
		data := val.([]interface{})

		// no success, be more verbose, you should know what`s happen, affected data, how to fix
		timelineMessage, err := convertRawMsgToTimelineMsg(data[1:])
		if err != nil {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "getByConsumerId"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = err
			logrus.WithError(derror).Errorf("redis error")
			continue
		}

		endLeaseMS, err := strconv.ParseUint(data[0].(string), 10, 64)
		if err != nil {
			var derror derrors.Dejaror
			derror.Module = derrors.ModuleStorage
			derror.Operation = "getByConsumerId"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = err
			logrus.WithError(derror).Errorf("redis error")
			continue
		}

		results = append(results, timeline.Lease{
			ExpirationTimestampMS: endLeaseMS,
			ConsumerID:            consumerID,
			Message:               newLeaseMessage(timelineMessage),
		})
	}

	return results, false, nil
}

func (c *Client) CountByStatus(ctx context.Context, request timeline.CountRequest) (uint64, error) {
	return 0, nil
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
