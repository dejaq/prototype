package redis

import (
	"context"
	"errors"
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

// Errors
var (
	ErrMessageAlreadyExists         = errors.New("messageId already exists, you can not set it again")
	ErrOperationFailRollbackSuccess = errors.New("fail operation, rollback with success")
	ErrOperationFailRollbackFail    = errors.New("fail operation, rollback fail")
	ErrStorageInternalError         = errors.New("internal error on storage")
	ErrUnknown                      = errors.New("no errors on storage level, no expected answer")
)

var operationToScript = map[string]string{
	"insert":          scripts.insert,
	"getAndLease":     scripts.getAndLease,
	"delete":          scripts.delete,
	"getByConsumerId": scripts.getByConsumerId,
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
			derror.Module = derrors.ModuleStorage
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

		insertErrors = append(insertErrors, derrors.MessageIDTuple{MessageID: msg.ID, Error: derror})
	}

	return insertErrors
}

// GetAndLease ...
func (c *Client) GetAndLease(ctx context.Context, timelineID []byte, buckets domain.BucketRange, consumerId []byte, leaseMs uint64, limit int, timeReferenceMS uint64) ([]timeline.Lease, bool, error) {
	// TODO use unsafe for a better conversion
	// TODO use transaction select, get message, lease

	var results []timeline.Lease

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

		// no success
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
			ConsumerID:            []byte(consumerId),
			Message:               timeline.NewLeaseMessage(timelineMessage),
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

// Lookup - not used at this time
func (c *Client) Lookup(ctx context.Context, timelineID []byte, messageIDs [][]byte) ([]timeline.Message, []derrors.MessageIDTuple) {
	return nil, nil
}

// Delete ...
func (c *Client) Delete(ctx context.Context, deleteMessages timeline.DeleteMessages) []derrors.MessageIDTuple {
	// TODO use transaction here MULTI and EXEC
	// TODO talk with @Adrian redundant return error and log it ???

	var deleteErrors []derrors.MessageIDTuple

	//for _, msg := range messages {
	//	keys := []string{
	//		c.createBucketKey("cluster_name", timelineID, msg.BucketID),
	//		msg.GetID(),
	//		// timeline to consumer (all consumer leased messages
	//		c.createTimelineKey("cluster_name", timelineID) + "::" + msg.GetLockConsumerID(),
	//	}
	//
	//	err := c.client.EvalSha(c.operationToScriptHash["delete"], keys).Err()
	//	if err != nil {
	//		var derror derrors.Dejaror
	//		derror.Module = derrors.ModuleStorage
	//		derror.Operation = "delete"
	//		derror.Message = err.Error()
	//		derror.ShouldRetry = true
	//		derror.WrappedErr = ErrStorageInternalError
	//		logrus.WithError(derror).Errorf("redis error")
	//		continue
	//	}
	//}

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
func (c *Client) SelectByConsumer(ctx context.Context, timelineID []byte, consumerID []byte, buckets domain.BucketRange, timeReferenceMS uint64) []timeline.Message {
	var messages []timeline.Message

	keys := []string{
		// timelineKey
		c.createTimelineKey("cluster_name", timelineID) + "::" + string(consumerID),
		// time reference in MS
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
		return []timeline.Message{}
	}

	dataCollection := data.([]interface{})
	for _, val := range dataCollection {
		data := val.([]interface{})
		timelineMessage, err := convertRawMsgToTimelineMsg(data)
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
		messages = append(messages, timelineMessage)
	}

	return messages
}

// SelectByProducer - not used at this time
func (c *Client) SelectByProducer(ctx context.Context, timelineID []byte, producerID []byte) []timeline.Message {
	return nil
}

type LeaseLu struct {
}
