package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/bgadrian/dejaq-broker/broker/domain"
	derrors "github.com/bgadrian/dejaq-broker/common/errors"
	"github.com/bgadrian/dejaq-broker/common/timeline"
	"github.com/go-redis/redis"
)

type Client struct {
	host   string
	client *redis.Client
	// map[operationType]redisHash
	operationToScriptHash map[string]string
}

// Errors
var (
	ErrMessageAlreadyExists         = errors.New("messageId already exists, you can not set it again")
	ErrOperationFailRollbackSuccess = errors.New("fail operation, rollback with success")
	ErrOperationFailRollbackFail    = errors.New("fail operation, rollback fail")
	ErrStorageInternalError         = errors.New("internal error on storage")
	ErrUnknown                      = errors.New("no errors on storage level, no expected answer")
)

var operationToScript = map[string]string{
	"insert":      scripts.insert,
	"getAndLease": scripts.getAndLease,
	"delete":      scripts.delete,
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

// Insert ...
func (c *Client) Insert(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	// TODO create batches and insert

	var insertErrors []derrors.MessageIDTuple

	for _, msg := range messages {
		bucketKey := c.createBucketKey("cluster_name", timelineID, msg.BucketID)
		messageKey := c.createMessageKey("cluster_name", timelineID, msg.BucketID, msg.ID)

		data := []string{
			"ID", msg.GetID(),
			"TimestampMS", strconv.FormatUint(msg.TimestampMS, 10),
			"BodyID", msg.GetBodyID(),
			"Body", msg.GetBody(),
			"ProducerGroupID", msg.GetProducerGroupID(),
			"LockConsumerID", msg.GetLockConsumerID(),
			"BucketID", strconv.Itoa(int(msg.BucketID)),
			"Version", strconv.Itoa(int(msg.Version)),
		}

		keys := []string{bucketKey, messageKey, data[1], data[3]}
		ok, err := c.client.EvalSha(c.operationToScriptHash["insert"], keys, data).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Module = 2
			derror.Operation = "insert"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = ErrStorageInternalError
			insertErrors = append(insertErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// continue on success
		if ok == "0" {
			continue
		}

		var derror derrors.Dejaror
		derror.Module = 2
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

		insertErrors = append(insertErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
	}

	return insertErrors
}

// GetAndLease ...
func (c *Client) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId string, leaseMs uint64, limit int, timeReferenceMS uint64) ([]timeline.PushLeases, bool, []derrors.MessageIDTuple) {
	// TODO use unsafe for a better conversion
	// TODO use transaction select, get message, lease

	var results []timeline.PushLeases
	var getErrors []derrors.MessageIDTuple

	keys := []string{
		// timelineKey
		c.createTimelineKey("cluster_name", timelineID),
		// time reference in MS
		strconv.FormatUint(timeReferenceMS, 10),
		// lease duration on MS
		strconv.FormatUint(leaseMs, 10),
		// max number of messages to get
		strconv.Itoa(limit),
		// consumerId
		consumerId,
	}

	// slice of buckets ids
	var argv []string
	for bucketID := buckets.Min(); bucketID <= buckets.Max(); bucketID++ {
		argv = append(argv, strconv.Itoa(int(bucketID)))
	}

	data, err := c.client.EvalSha(c.operationToScriptHash["getAndLease"], keys, argv).Result()
	if err != nil {
		var derror derrors.Dejaror
		derror.Module = 2
		derror.Operation = "getAndLease"
		derror.Message = err.Error()
		derror.ShouldRetry = true
		derror.WrappedErr = ErrStorageInternalError
		getErrors = append(getErrors, derrors.MessageIDTuple{Error: derror})
	}

	// TODO not the best practice, find a better solution here to remove interface mess
	d := data.([]interface{})
	for _, val := range d {
		v := val.([]interface{})

		// get data
		msgID := v[0].(string)
		code := v[1].(string)
		endLeaseMS := uint64(v[2].(int64))
		message := v[3].([]interface{})

		// no success
		if code != "0" {
			var derror derrors.Dejaror
			derror.Module = 2
			derror.Operation = "getAndLease"

			switch code {
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

			getErrors = append(getErrors, derrors.MessageIDTuple{Error: derror})

			continue
		}

		timelineMessage, err := convertRawMsgToTimelineMsg(message)

		if err != nil {
			var derror derrors.Dejaror
			derror.Module = 2
			derror.Operation = "getAndLease"
			derror.Message = err.Error()
			derror.ShouldRetry = true
			derror.WrappedErr = err
			getErrors = append(getErrors, derrors.MessageIDTuple{MessageID: []byte(msgID), Error: derror})
			continue
		}

		results = append(results, timeline.PushLeases{
			ExpirationTimestampMS: endLeaseMS,
			ConsumerID:            []byte(consumerId),
			Message:               timeline.NewLeaseMessage(timelineMessage),
		})
	}

	return results, false, getErrors
}

func convertRawMsgToTimelineMsg(rawMessage []interface{}) (timeline.Message, error) {
	// TODO implement errors
	// TODO find better type conversion

	var message timeline.Message

	// TODO they are not came on same order (on container order was respected)
	var key string
	for i, v := range rawMessage {
		// get key
		if i%2 == 0 {
			key = v.(string)
			continue
		}

		switch key {
		case "ID":
			message.ID = []byte(fmt.Sprintf("%v", v))
		case "TimestampMS":
			timestamp, _ := strconv.ParseUint(fmt.Sprintf("%v", v), 10, 64)
			message.TimestampMS = timestamp
		case "BodyID":
			message.BodyID = []byte(fmt.Sprintf("%v", v))
		case "Body":
			message.Body = []byte(fmt.Sprintf("%v", v))
		case "ProducerGroupID":
			message.ProducerGroupID = []byte(fmt.Sprintf("%v", v))
		case "LockConsumerID":
			message.LockConsumerID = []byte(fmt.Sprintf("%v", v))
		case "BucketID":
			bucketIdUint16, _ := strconv.ParseUint(fmt.Sprintf("%v", v), 10, 16)
			message.BucketID = uint16(bucketIdUint16)
		case "Version":
			version, _ := strconv.ParseUint(fmt.Sprintf("%v", v), 10, 16)
			message.Version = uint16(version)
		}
	}

	return message, nil
}

// Lookup - not used at this time
func (c *Client) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

// Delete ...
func (c *Client) Delete(ctx context.Context, timelineID []byte, messages []timeline.Message) []derrors.MessageIDTuple {
	// TODO use transaction here MULTI and EXEC

	var deleteErrors []derrors.MessageIDTuple

	for _, msg := range messages {
		// deleted from sorted set
		bucketKey := c.createBucketKey("cluster_name", timelineID, msg.BucketID)
		ok, err := c.client.ZRem(bucketKey, msg.GetID()).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			deleteErrors = append(deleteErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		if ok != 1 {
			var derror derrors.Dejaror
			derror.Message = "MessageId was not deleted from redis"
			deleteErrors = append(deleteErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		// delete message data from hashMap
		messageKey := c.createMessageKey("cluster_name:", timelineID, msg.BucketID, msg.ID)
		ok, err = c.client.HDel(
			messageKey,
			"ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version",
		).Result()

		if err != nil {
			var derror derrors.Dejaror
			derror.Message = err.Error()
			deleteErrors = append(deleteErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}

		if ok != 8 {
			var derror derrors.Dejaror
			derror.Message = "Message data was not deleted from redis"
			deleteErrors = append(deleteErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
		}
	}

	return deleteErrors
}

// CountByRange - not used at this time
func (c *Client) CountByRange(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// CountByRangeProcessing - not used at this time
func (c *Client) CountByRangeProcessing(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// CountByRangeWaiting - not used at this time
func (c *Client) CountByRangeWaiting(ctx context.Context, timelineID []byte, a, b uint64) uint64 {
	return 0
}

// SelectByConsumer - not used at this time
func (c *Client) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte) []timeline.Message {
	return nil
}

// SelectByProducer - not used at this time
func (c *Client) SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message {
	return nil
}
